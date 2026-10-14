#include "streamforge/broker/broker_service.hpp"
#include "streamforge/network/tcp_server.hpp"
#include "streamforge/protocol/frame_codec.hpp"

#include <array>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <gtest/gtest.h>

namespace streamforge::network {
namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;
void close_socket(NativeSocket socket) { closesocket(socket); }
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidSocket = -1;
void close_socket(NativeSocket socket) { close(socket); }
#endif

class Socket {
 public:
  Socket() = default;
  explicit Socket(NativeSocket value) : value_(value) {}
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept : value_(std::exchange(other.value_, kInvalidSocket)) {}
  Socket& operator=(Socket&& other) noexcept {
    if (this != &other) {
      if (value_ != kInvalidSocket) close_socket(value_);
      value_ = std::exchange(other.value_, kInvalidSocket);
    }
    return *this;
  }
  ~Socket() { if (value_ != kInvalidSocket) close_socket(value_); }
  [[nodiscard]] NativeSocket get() const { return value_; }
  [[nodiscard]] bool valid() const { return value_ != kInvalidSocket; }
 private:
  NativeSocket value_{kInvalidSocket};
};

class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::filesystem::temp_directory_path() /
            ("streamforge-tcp-test-" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  void TearDown() override { std::filesystem::remove_all(root_); }
  std::filesystem::path root_;
};

Socket connect_to(std::uint16_t port) {
  const auto socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket == kInvalidSocket) throw std::runtime_error("cannot create test socket");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    close_socket(socket);
    throw std::runtime_error("cannot connect to test server");
  }
  return Socket(socket);
}

void send_bytes(NativeSocket socket, const std::vector<std::byte>& bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
#ifdef _WIN32
    const auto result = send(socket, reinterpret_cast<const char*>(bytes.data() + sent),
                             static_cast<int>(bytes.size() - sent), 0);
#else
    const auto result = send(socket, bytes.data() + sent, bytes.size() - sent, 0);
#endif
    ASSERT_GT(result, 0);
    sent += static_cast<std::size_t>(result);
  }
}

void send_bytes_or_throw(NativeSocket socket, const std::vector<std::byte>& bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
#ifdef _WIN32
    const auto result = send(socket, reinterpret_cast<const char*>(bytes.data() + sent),
                             static_cast<int>(bytes.size() - sent), 0);
#else
    const auto result = send(socket, bytes.data() + sent, bytes.size() - sent, 0);
#endif
    if (result <= 0) throw std::runtime_error("test client could not send request");
    sent += static_cast<std::size_t>(result);
  }
}

void set_client_timeouts(NativeSocket socket) {
#ifdef _WIN32
  const DWORD timeout_ms = 2000;
  if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout_ms),
                 sizeof(timeout_ms)) != 0 ||
      setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout_ms),
                 sizeof(timeout_ms)) != 0) {
    throw std::runtime_error("cannot configure test socket timeout");
  }
#else
  const timeval timeout{2, 0};
  if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
      setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
    throw std::runtime_error("cannot configure test socket timeout");
  }
#endif
}

protocol::Frame receive_frame(NativeSocket socket) {
  protocol::FrameDecoder decoder;
  std::vector<std::byte> buffer(128);
  while (!decoder.has_frame()) {
#ifdef _WIN32
    const auto received = recv(socket, reinterpret_cast<char*>(buffer.data()),
                               static_cast<int>(buffer.size()), 0);
#else
    const auto received = recv(socket, buffer.data(), buffer.size(), 0);
#endif
    if (received <= 0) throw std::runtime_error("server closed test connection");
    if (!decoder.feed(std::span{buffer}.first(static_cast<std::size_t>(received)))) {
      throw std::runtime_error("server returned an invalid frame");
    }
  }
  return decoder.take_frame();
}

bool closes_within(NativeSocket socket, std::chrono::milliseconds timeout) {
  fd_set readable{};
  FD_ZERO(&readable);
  FD_SET(socket, &readable);
  timeval wait{
      static_cast<long>(timeout.count() / 1000),
      static_cast<long>((timeout.count() % 1000) * 1000),
  };
#ifdef _WIN32
  const auto ready = select(0, &readable, nullptr, nullptr, &wait);
#else
  const auto ready = select(socket + 1, &readable, nullptr, nullptr, &wait);
#endif
  if (ready <= 0) return false;
  std::byte byte{};
#ifdef _WIN32
  return recv(socket, reinterpret_cast<char*>(&byte), 1, 0) <= 0;
#else
  return recv(socket, &byte, 1, 0) <= 0;
#endif
}

void expect_ping(NativeSocket socket, std::uint64_t request_id) {
  const protocol::Frame request{protocol::FrameHeader{.operation = protocol::Operation::kPing,
                                                       .request_id = request_id},
                                {}};
  send_bytes(socket, protocol::encode_frame(request));
  const auto response = receive_frame(socket);
  if (response.header.status != protocol::Status::kOk ||
      response.header.request_id != request_id) {
    throw std::runtime_error("unexpected ping response");
  }
}

std::vector<std::byte> create_topic_payload(const std::string& name, std::uint32_t partitions) {
  std::vector<std::byte> payload;
  payload.push_back(static_cast<std::byte>((name.size() >> 8U) & 0xFFU));
  payload.push_back(static_cast<std::byte>(name.size() & 0xFFU));
  for (const auto character : name) payload.push_back(static_cast<std::byte>(character));
  for (int shift = 24; shift >= 0; shift -= 8) {
    payload.push_back(static_cast<std::byte>((partitions >> shift) & 0xFFU));
  }
  return payload;
}

void append_u32(std::vector<std::byte>& bytes, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
}
void append_u64(std::vector<std::byte>& bytes, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
}
std::vector<std::byte> topic_prefix(const std::string& name, std::uint32_t partition) {
  auto bytes = create_topic_payload(name, partition);
  bytes.resize(2U + name.size());
  append_u32(bytes, partition);
  return bytes;
}

std::vector<std::byte> single_record_payload(const std::string& topic, std::uint32_t partition,
                                             const std::string& value) {
  auto payload = topic_prefix(topic, partition);
  payload.push_back(std::byte{0});
  append_u32(payload, 1);
  append_u64(payload, 0);
  append_u32(payload, 0);
  append_u32(payload, static_cast<std::uint32_t>(value.size()));
  for (const auto character : value) payload.push_back(static_cast<std::byte>(character));
  return payload;
}

protocol::Frame request_and_receive(NativeSocket socket, protocol::Operation operation,
                                    std::uint64_t request_id, std::vector<std::byte> payload) {
  send_bytes_or_throw(socket, protocol::encode_frame({
      protocol::FrameHeader{.operation = operation, .request_id = request_id}, std::move(payload)}));
  const auto response = receive_frame(socket);
  if (response.header.request_id != request_id || response.header.status != protocol::Status::kOk) {
    throw std::runtime_error("test client received an unsuccessful response");
  }
  return response;
}

std::uint64_t response_offset(const std::vector<std::byte>& payload, std::size_t offset) {
  if (payload.size() != 16U) throw std::runtime_error("invalid produce response length");
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<std::uint64_t>(payload[offset + index]);
  }
  return value;
}

std::string as_string(const std::vector<std::byte>& value) {
  std::string result;
  result.reserve(value.size());
  for (const auto byte : value) result.push_back(static_cast<char>(byte));
  return result;
}

class ThreadFailures {
 public:
  void capture_current() {
    std::lock_guard lock(mutex_);
    if (!failure_) failure_ = std::current_exception();
  }
  void rethrow_if_any() const {
    std::lock_guard lock(mutex_);
    if (failure_) std::rethrow_exception(failure_);
  }
 private:
  mutable std::mutex mutex_;
  std::exception_ptr failure_;
};

TEST_F(TcpServerTest, DecodesFragmentedPingAndDispatchesCreateTopic) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  server.start();
  ASSERT_NE(server.port(), 0U);
  const auto client = connect_to(server.port());

  const protocol::Frame ping{protocol::FrameHeader{.operation = protocol::Operation::kPing,
                                                    .request_id = 41},
                             {std::byte{0xA1}}};
  const auto encoded_ping = protocol::encode_frame(ping);
  send_bytes(client.get(), {encoded_ping.begin(), encoded_ping.begin() + 3});
  send_bytes(client.get(), {encoded_ping.begin() + 3, encoded_ping.end()});
  const auto ping_response = receive_frame(client.get());
  EXPECT_EQ(ping_response.header.request_id, 41U);
  EXPECT_EQ(ping_response.header.status, protocol::Status::kOk);
  EXPECT_EQ(ping_response.payload, ping.payload);

  const protocol::Frame create{protocol::FrameHeader{.operation = protocol::Operation::kCreateTopic,
                                                      .request_id = 42},
                               create_topic_payload("events", 2)};
  send_bytes(client.get(), protocol::encode_frame(create));
  const auto create_response = receive_frame(client.get());
  EXPECT_EQ(create_response.header.request_id, 42U);
  EXPECT_EQ(create_response.header.status, protocol::Status::kOk);
  EXPECT_EQ(broker.fetch("events", 1, 0, 1).next_offset, 0U);
  server.stop();
}

TEST_F(TcpServerTest, ReturnsStableStatusForReservedOperation) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  server.start();
  const auto client = connect_to(server.port());
  const protocol::Frame request{protocol::FrameHeader{.operation = protocol::Operation::kDescribeTopic,
                                                       .request_id = 9}, {}};
  send_bytes(client.get(), protocol::encode_frame(request));
  const auto response = receive_frame(client.get());
  EXPECT_EQ(response.header.request_id, 9U);
  EXPECT_EQ(response.header.status, protocol::Status::kUnknownOperation);
  server.stop();
}

TEST_F(TcpServerTest, ProducesAndFetchesRecordsThroughTcp) {
  broker::BrokerService broker(root_); broker.recover(); broker.create_topic("events", 1);
  TcpServer server(broker); server.start(); const auto client = connect_to(server.port());
  auto produce_payload = topic_prefix("events", 0);
  produce_payload.push_back(std::byte{0}); append_u32(produce_payload, 1);
  append_u64(produce_payload, 123); append_u32(produce_payload, 1); append_u32(produce_payload, 2);
  produce_payload.push_back(std::byte{'k'}); produce_payload.push_back(std::byte{'v'}); produce_payload.push_back(std::byte{'1'});
  send_bytes(client.get(), protocol::encode_frame({protocol::FrameHeader{.operation = protocol::Operation::kProduceBatch, .request_id = 10}, produce_payload}));
  EXPECT_EQ(receive_frame(client.get()).header.status, protocol::Status::kOk);
  auto fetch_payload = topic_prefix("events", 0); append_u64(fetch_payload, 0); append_u32(fetch_payload, 1);
  send_bytes(client.get(), protocol::encode_frame({protocol::FrameHeader{.operation = protocol::Operation::kFetch, .request_id = 11}, fetch_payload}));
  const auto response = receive_frame(client.get());
  ASSERT_EQ(response.header.status, protocol::Status::kOk);
  ASSERT_GE(response.payload.size(), 12U);
  EXPECT_EQ(static_cast<std::uint8_t>(response.payload[11]), 1U);  // One returned record.
  server.stop();
}

TEST_F(TcpServerTest, StopsIdempotentlyReportsPortAndRestarts) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);

  server.start();
  const auto first_port = server.port();
  ASSERT_NE(first_port, 0U);
  EXPECT_THROW(server.start(), std::logic_error);
  server.stop();
  EXPECT_EQ(server.port(), 0U);
  server.stop();
  EXPECT_EQ(server.port(), 0U);

  server.start();
  ASSERT_NE(server.port(), 0U);
  const auto client = connect_to(server.port());
  expect_ping(client.get(), 77);
  server.stop();
}

TEST_F(TcpServerTest, StopsIdleConnectionsAndClosesTheirSockets) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  server.start();
  const auto client = connect_to(server.port());
  expect_ping(client.get(), 88);  // Confirms the accepted socket is registered.

  server.stop();
  EXPECT_TRUE(closes_within(client.get(), std::chrono::seconds(2)));
  EXPECT_EQ(server.port(), 0U);
}

TEST_F(TcpServerTest, StopsConnectionWaitingForAnIncompleteFrame) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  server.start();
  const auto client = connect_to(server.port());
  expect_ping(client.get(), 89);  // Confirms the accepted socket is registered.

  // Leave the server's frame decoder waiting for the rest of a request.
  send_bytes(client.get(), {std::byte{0x00}, std::byte{0x00}});
  server.stop();
  EXPECT_TRUE(closes_within(client.get(), std::chrono::seconds(2)));
  EXPECT_EQ(server.port(), 0U);
}

TEST_F(TcpServerTest, RejectsConnectionsBeyondConfiguredMaximum) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker, TcpServer::Configuration{.maximum_active_connections = 1});
  server.start();
  const auto first_client = connect_to(server.port());
  expect_ping(first_client.get(), 91);  // Ensures the one permitted slot is active.
  const auto rejected_client = connect_to(server.port());

  EXPECT_TRUE(closes_within(rejected_client.get(), std::chrono::seconds(2)));
  expect_ping(first_client.get(), 92);
  server.stop();
}

TEST_F(TcpServerTest, RepeatsStartStopCycles) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  for (std::uint64_t cycle = 0; cycle < 3; ++cycle) {
    server.start();
    const auto client = connect_to(server.port());
    expect_ping(client.get(), 100 + cycle);
    server.stop();
    EXPECT_EQ(server.port(), 0U);
  }
}

TEST_F(TcpServerTest, ConcurrentClientsProduceContiguousOffsetsToOnePartition) {
  broker::BrokerService broker(root_);
  broker.recover();
  broker.create_topic("events", 1);
  TcpServer server(broker);
  server.start();

  constexpr std::size_t kClients = 6;
  constexpr std::size_t kRecordsPerClient = 8;
  std::barrier start(static_cast<std::ptrdiff_t>(kClients + 1));
  ThreadFailures failures;
  std::vector<std::thread> clients;
  for (std::size_t client_index = 0; client_index < kClients; ++client_index) {
    clients.emplace_back([&, client_index] {
      Socket client;
      try {
        client = connect_to(server.port());
        set_client_timeouts(client.get());
      } catch (...) {
        failures.capture_current();
      }
      start.arrive_and_wait();
      if (!client.valid()) return;
      try {
        for (std::size_t record_index = 0; record_index < kRecordsPerClient; ++record_index) {
          const auto value = "client-" + std::to_string(client_index) + "-record-" +
                             std::to_string(record_index);
          const auto response = request_and_receive(
              client.get(), protocol::Operation::kProduceBatch,
              static_cast<std::uint64_t>(client_index * 1000 + record_index + 1),
              single_record_payload("events", 0, value));
          if (response_offset(response.payload, 0) != response_offset(response.payload, 8)) {
            throw std::runtime_error("single-record append returned an invalid offset range");
          }
        }
      } catch (...) {
        failures.capture_current();
      }
    });
  }
  start.arrive_and_wait();
  for (auto& client : clients) client.join();
  ASSERT_NO_THROW(failures.rethrow_if_any());

  const auto fetched = broker.fetch("events", 0, 0, kClients * kRecordsPerClient);
  ASSERT_EQ(fetched.records.size(), kClients * kRecordsPerClient);
  std::set<std::string> expected;
  for (std::size_t client_index = 0; client_index < kClients; ++client_index) {
    for (std::size_t record_index = 0; record_index < kRecordsPerClient; ++record_index) {
      expected.insert("client-" + std::to_string(client_index) + "-record-" +
                      std::to_string(record_index));
    }
  }
  std::set<std::string> actual;
  for (std::size_t index = 0; index < fetched.records.size(); ++index) {
    EXPECT_EQ(fetched.records[index].offset, index);
    actual.insert(as_string(fetched.records[index].value));
  }
  EXPECT_EQ(actual, expected);
  server.stop();
}

TEST_F(TcpServerTest, ConcurrentClientsProduceToSeparatePartitions) {
  broker::BrokerService broker(root_);
  broker.recover();
  constexpr std::size_t kPartitions = 4;
  constexpr std::size_t kRecordsPerClient = 9;
  broker.create_topic("partitioned", kPartitions);
  TcpServer server(broker);
  server.start();

  std::barrier start(static_cast<std::ptrdiff_t>(kPartitions + 1));
  ThreadFailures failures;
  std::vector<std::thread> clients;
  for (std::size_t partition = 0; partition < kPartitions; ++partition) {
    clients.emplace_back([&, partition] {
      Socket client;
      try {
        client = connect_to(server.port());
        set_client_timeouts(client.get());
      } catch (...) {
        failures.capture_current();
      }
      start.arrive_and_wait();
      if (!client.valid()) return;
      try {
        for (std::size_t record = 0; record < kRecordsPerClient; ++record) {
          const auto value = "partition-" + std::to_string(partition) + "-record-" +
                             std::to_string(record);
          const auto response = request_and_receive(
              client.get(), protocol::Operation::kProduceBatch,
              static_cast<std::uint64_t>(partition * 1000 + record + 1),
              single_record_payload("partitioned", static_cast<std::uint32_t>(partition), value));
          if (response_offset(response.payload, 0) != response_offset(response.payload, 8)) {
            throw std::runtime_error("single-record append returned an invalid offset range");
          }
        }
      } catch (...) {
        failures.capture_current();
      }
    });
  }
  start.arrive_and_wait();
  for (auto& client : clients) client.join();
  ASSERT_NO_THROW(failures.rethrow_if_any());

  for (std::size_t partition = 0; partition < kPartitions; ++partition) {
    const auto fetched = broker.fetch("partitioned", static_cast<std::uint32_t>(partition), 0,
                                      kRecordsPerClient);
    ASSERT_EQ(fetched.records.size(), kRecordsPerClient);
    std::set<std::string> expected;
    std::set<std::string> actual;
    for (std::size_t record = 0; record < kRecordsPerClient; ++record) {
      expected.insert("partition-" + std::to_string(partition) + "-record-" +
                      std::to_string(record));
      EXPECT_EQ(fetched.records[record].offset, record);
      actual.insert(as_string(fetched.records[record].value));
    }
    EXPECT_EQ(actual, expected);
  }
  server.stop();
}

TEST_F(TcpServerTest, ExportsPrometheusMetricsThroughProtocol) {
  broker::BrokerService broker(root_);
  broker.recover();
  TcpServer server(broker);
  server.start();
  ASSERT_NE(server.port(), 0U);
  const auto client = connect_to(server.port());

  // Produce a record to generate metric activity
  const auto create = request_and_receive(client.get(), protocol::Operation::kCreateTopic, 1,
                                          create_topic_payload("metrics_topic", 1));
  EXPECT_EQ(create.header.status, protocol::Status::kOk);

  const auto produce = request_and_receive(client.get(), protocol::Operation::kProduceBatch, 2,
                                           single_record_payload("metrics_topic", 0, "metric_sample"));
  EXPECT_EQ(produce.header.status, protocol::Status::kOk);

  // Request metrics via kGetMetrics (operation 10)
  const auto metrics_frame = request_and_receive(client.get(), protocol::Operation::kGetMetrics, 3, {});
  EXPECT_EQ(metrics_frame.header.status, protocol::Status::kOk);
  const std::string text = as_string(metrics_frame.payload);

  EXPECT_NE(text.find("eventstream_append_operations_total"), std::string::npos);
  EXPECT_NE(text.find("eventstream_active_connections"), std::string::npos);
  EXPECT_NE(text.find("eventstream_append_latency_ms"), std::string::npos);

  server.stop();
}

}  // namespace
}  // namespace streamforge::network


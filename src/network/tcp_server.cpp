#include "streamforge/network/tcp_server.hpp"
#include "streamforge/observability/metrics_registry.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace streamforge::network {
namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;

void close_socket(NativeSocket socket) noexcept { closesocket(socket); }
void wake_socket(NativeSocket socket) noexcept { shutdown(socket, SD_BOTH); }

class WinsockSession {
 public:
  WinsockSession() {
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) throw std::runtime_error("WSAStartup failed");
  }
  ~WinsockSession() { WSACleanup(); }
};
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidSocket = -1;

void close_socket(NativeSocket socket) noexcept { close(socket); }
void wake_socket(NativeSocket socket) noexcept { shutdown(socket, SHUT_RDWR); }
#endif

NativeSocket as_socket(std::intptr_t socket) { return static_cast<NativeSocket>(socket); }
std::intptr_t as_integer(NativeSocket socket) { return static_cast<std::intptr_t>(socket); }

bool send_all(NativeSocket socket, const std::vector<std::byte>& bytes) noexcept {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
#ifdef _WIN32
    const auto result = send(socket, reinterpret_cast<const char*>(bytes.data() + sent),
                             static_cast<int>(bytes.size() - sent), 0);
#else
    const auto result = send(socket, bytes.data() + sent, bytes.size() - sent, 0);
#endif
    if (result <= 0) return false;
    sent += static_cast<std::size_t>(result);
  }
  return true;
}

std::uint16_t read_u16(const std::vector<std::byte>& input, std::size_t offset) {
  return (static_cast<std::uint16_t>(input[offset]) << 8U) |
         static_cast<std::uint16_t>(input[offset + 1]);
}

std::uint32_t read_u32(const std::vector<std::byte>& input, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = (value << 8U) | static_cast<std::uint32_t>(input[offset + index]);
  }
  return value;
}

std::uint64_t read_u64(const std::vector<std::byte>& input, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) value = (value << 8U) | static_cast<std::uint64_t>(input[offset + index]);
  return value;
}

void append_u32(std::vector<std::byte>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
}

void append_u64(std::vector<std::byte>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
}

}  // namespace

TcpServer::TcpServer(broker::BrokerService& broker, Configuration configuration)
    : broker_(broker), configuration_(configuration) {
  if (configuration_.read_buffer_size == 0) throw std::invalid_argument("read buffer cannot be empty");
  if (configuration_.maximum_active_connections == 0) {
    throw std::invalid_argument("maximum active connections cannot be zero");
  }
}

TcpServer::~TcpServer() { stop(); }

void TcpServer::start() {
  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  if (running_ || stopping_ || accept_thread_.joinable()) {
    throw std::logic_error("TCP server is already running or shutting down");
  }
  try {
#ifdef _WIN32
    static WinsockSession winsock;
#endif
    const auto socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket == kInvalidSocket) throw std::runtime_error("cannot create listening socket");
    listen_socket_ = as_integer(socket);

    int reuse_address = 1;
    (void)setsockopt(socket, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&reuse_address), sizeof(reuse_address));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(configuration_.port);
    if (bind(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        listen(socket, SOMAXCONN) != 0) {
      close_socket(socket);
      listen_socket_ = -1;
      throw std::runtime_error("cannot bind or listen on TCP port");
    }
    sockaddr_in bound{};
#ifdef _WIN32
    int bound_size = sizeof(bound);
#else
    socklen_t bound_size = sizeof(bound);
#endif
    if (getsockname(socket, reinterpret_cast<sockaddr*>(&bound), &bound_size) != 0) {
      close_socket(socket);
      listen_socket_ = -1;
      throw std::runtime_error("cannot determine TCP port");
    }
    bound_port_ = ntohs(bound.sin_port);
    running_ = true;
    accept_thread_ = std::thread(&TcpServer::accept_loop, this);
  } catch (...) {
    running_ = false;
    bound_port_ = 0;
    if (listen_socket_ != -1) {
      close_socket(as_socket(listen_socket_));
      listen_socket_ = -1;
    }
    throw;
  }
}

void TcpServer::stop() noexcept {
  std::thread accept_thread;
  std::intptr_t listen_socket = -1;
  {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (stopping_ || !running_) return;
    stopping_ = true;
    running_ = false;
    listen_socket = listen_socket_;
    std::swap(accept_thread, accept_thread_);
  }

  // shutdown() wakes select()/accept(); the descriptor stays open until its
  // owner thread exits, avoiding close-versus-accept descriptor races.
  if (listen_socket != -1) wake_socket(as_socket(listen_socket));
  {
    std::lock_guard lock(sockets_mutex_);
    for (const auto& connection : client_connections_) {
      if (connection.socket != -1) wake_socket(as_socket(connection.socket));
    }
  }
  if (accept_thread.joinable()) accept_thread.join();

  std::vector<ClientConnection> client_connections;
  {
    std::lock_guard lock(sockets_mutex_);
    for (const auto& connection : client_connections_) {
      if (connection.socket != -1) wake_socket(as_socket(connection.socket));
    }
    client_connections.swap(client_connections_);
  }
  for (auto& connection : client_connections) {
    if (connection.thread.joinable()) connection.thread.join();
  }
  if (listen_socket != -1) close_socket(as_socket(listen_socket));

  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  listen_socket_ = -1;
  bound_port_ = 0;
  stopping_ = false;
}

std::uint16_t TcpServer::port() const noexcept {
  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  return running_ ? bound_port_ : 0;
}

void TcpServer::set_profiling_enabled(bool enabled) noexcept {
  profile_operations_ = 0; profile_processing_ns_ = 0;
  profiling_enabled_ = enabled;
}

TcpDispatchProfile TcpServer::profile() const noexcept {
  return {profile_operations_.load(), profile_processing_ns_.load()};
}

void TcpServer::accept_loop() {
  while (running_) {
    reap_finished_clients();
    std::intptr_t listen_socket = -1;
    {
      std::lock_guard lifecycle_lock(lifecycle_mutex_);
      if (!running_ || listen_socket_ == -1) return;
      listen_socket = listen_socket_;
    }
    fd_set readable{};
    FD_ZERO(&readable);
    FD_SET(as_socket(listen_socket), &readable);
    timeval timeout{0, 100000};  // Check shutdown at least every 100 ms.
#ifdef _WIN32
    const auto ready = select(0, &readable, nullptr, nullptr, &timeout);
#else
    const auto ready = select(as_socket(listen_socket) + 1, &readable, nullptr, nullptr, &timeout);
#endif
    if (ready <= 0) continue;
    if (!running_) return;
    const auto client = accept(as_socket(listen_socket), nullptr, nullptr);
    if (client == kInvalidSocket) {
      if (!running_) return;
      continue;
    }
    if (!running_) {
      close_socket(client);
      return;
    }
    std::lock_guard lock(sockets_mutex_);
    if (!running_) {
      close_socket(client);
      return;
    }
    const auto active_connections = static_cast<std::size_t>(std::count_if(
        client_connections_.begin(), client_connections_.end(),
        [](const ClientConnection& connection) { return connection.socket != -1; }));
    if (active_connections >= configuration_.maximum_active_connections) {
      close_socket(client);
      continue;
    }
    try {
      client_connections_.emplace_back();
      auto& connection = client_connections_.back();
      connection.socket = as_integer(client);
      connection.thread = std::thread(&TcpServer::client_loop, this, connection.socket);
      observability::MetricsRegistry::instance().record_connection_opened();
    } catch (...) {
      if (!client_connections_.empty() && client_connections_.back().socket == as_integer(client)) {
        client_connections_.pop_back();
      }
      close_socket(client);
    }
  }
}

void TcpServer::reap_finished_clients() {
  while (true) {
    std::thread finished_thread;
    {
      std::lock_guard lock(sockets_mutex_);
      const auto finished = std::find_if(client_connections_.begin(), client_connections_.end(),
                                         [](const ClientConnection& connection) {
                                           return connection.socket == -1;
                                         });
      if (finished == client_connections_.end()) return;
      finished_thread = std::move(finished->thread);
      client_connections_.erase(finished);
    }
    if (finished_thread.joinable()) finished_thread.join();
  }
}

void TcpServer::client_loop(std::intptr_t client_socket) noexcept {
  const auto socket = as_socket(client_socket);
  std::vector<std::byte> buffer(configuration_.read_buffer_size);
  protocol::FrameDecoder decoder(configuration_.maximum_frame_body_size);
  while (running_) {
    fd_set readable{};
    FD_ZERO(&readable);
    FD_SET(socket, &readable);
    timeval timeout{0, 100000};  // Avoid an unbounded blocking read during stop().
#ifdef _WIN32
    const auto ready = select(0, &readable, nullptr, nullptr, &timeout);
#else
    const auto ready = select(socket + 1, &readable, nullptr, nullptr, &timeout);
#endif
    if (ready == 0) continue;
    if (ready < 0) break;
#ifdef _WIN32
    const auto received = recv(socket, reinterpret_cast<char*>(buffer.data()),
                               static_cast<int>(buffer.size()), 0);
#else
    const auto received = recv(socket, buffer.data(), buffer.size(), 0);
#endif
    if (received <= 0) break;
    if (!decoder.feed(std::span{buffer}.first(static_cast<std::size_t>(received)))) break;
    while (decoder.has_frame()) {
      const auto response = dispatch(decoder.take_frame());
      if (!send_all(socket, protocol::encode_frame(response))) {
        break;
      }
    }
  }
  close_socket(socket);
  observability::MetricsRegistry::instance().record_connection_closed();
  std::lock_guard lock(sockets_mutex_);
  const auto connection = std::find_if(client_connections_.begin(), client_connections_.end(),
                                       [client_socket](const ClientConnection& candidate) {
                                         return candidate.socket == client_socket;
                                       });
  if (connection != client_connections_.end()) connection->socket = -1;
}

protocol::Frame TcpServer::dispatch(const protocol::Frame& request) {
  protocol::Frame response{request.header, {}};
  response.header.status = protocol::Status::kOk;
  try {
    if (request.header.status != protocol::Status::kOk) {
      throw std::invalid_argument("requests must use an OK status field");
    }
    switch (request.header.operation) {
      case protocol::Operation::kPing:
        response.payload = request.payload;
        break;
      case protocol::Operation::kCreateTopic: {
        if (request.payload.size() < 6) throw std::invalid_argument("truncated create-topic payload");
        const auto name_size = read_u16(request.payload, 0);
        if (name_size == 0 || request.payload.size() != static_cast<std::size_t>(name_size) + 6U) {
          throw std::invalid_argument("invalid create-topic payload length");
        }
        const std::string name(reinterpret_cast<const char*>(request.payload.data() + 2), name_size);
        const auto partitions = read_u32(request.payload, static_cast<std::size_t>(name_size) + 2U);
        broker_.create_topic(name, partitions);
        break;
      }
      case protocol::Operation::kProduceBatch: {
        const auto processing_started = std::chrono::steady_clock::now();
        if (request.payload.size() < 11) throw std::invalid_argument("truncated produce payload");
        const auto name_size = read_u16(request.payload, 0);
        std::size_t cursor = 2U + name_size;
        if (name_size == 0 || cursor + 9U > request.payload.size()) throw std::invalid_argument("invalid produce topic");
        const std::string name(reinterpret_cast<const char*>(request.payload.data() + 2), name_size);
        const auto partition = read_u32(request.payload, cursor); cursor += 4;
        const auto mode = static_cast<std::uint8_t>(request.payload[cursor++]);
        const auto count = read_u32(request.payload, cursor); cursor += 4;
        if (mode != 0 || count == 0 || count > 10000) throw std::invalid_argument("invalid produce mode or count");
        std::vector<storage::Record> records;
        records.reserve(count);
        for (std::uint32_t index = 0; index < count; ++index) {
          if (cursor + 16U > request.payload.size()) throw std::invalid_argument("truncated produce record");
          const auto timestamp = static_cast<std::int64_t>(read_u64(request.payload, cursor)); cursor += 8;
          const auto key_size = read_u32(request.payload, cursor); cursor += 4;
          const auto value_size = read_u32(request.payload, cursor); cursor += 4;
          if (key_size > request.payload.size() - cursor || value_size > request.payload.size() - cursor - key_size) throw std::invalid_argument("invalid produce record lengths");
          storage::Record record{.timestamp_unix_ms = timestamp};
          record.key.assign(request.payload.begin() + static_cast<std::ptrdiff_t>(cursor), request.payload.begin() + static_cast<std::ptrdiff_t>(cursor + key_size)); cursor += key_size;
          record.value.assign(request.payload.begin() + static_cast<std::ptrdiff_t>(cursor), request.payload.begin() + static_cast<std::ptrdiff_t>(cursor + value_size)); cursor += value_size;
          records.push_back(std::move(record));
        }
        if (cursor != request.payload.size()) throw std::invalid_argument("trailing produce bytes");
        const auto app_start = std::chrono::steady_clock::now();
        const auto appended = broker_.produce(name, partition, std::move(records), storage::AcknowledgementMode::kAppended);
        const auto app_dur = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - app_start).count());
        observability::MetricsRegistry::instance().record_append(app_dur, count, cursor);
        append_u64(response.payload, appended.first_offset);
        append_u64(response.payload, appended.last_offset);
        if (profiling_enabled_) {
          profile_operations_.fetch_add(1, std::memory_order_relaxed);
          profile_processing_ns_.fetch_add(static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - processing_started).count()), std::memory_order_relaxed);
        }
        break;
      }
      case protocol::Operation::kFetch: {
        const auto fetch_start = std::chrono::steady_clock::now();
        if (request.payload.size() < 18) throw std::invalid_argument("truncated fetch payload");
        const auto name_size = read_u16(request.payload, 0);
        const auto required = static_cast<std::size_t>(name_size) + 18U;
        if (name_size == 0 || request.payload.size() != required) throw std::invalid_argument("invalid fetch payload length");
        const std::string name(reinterpret_cast<const char*>(request.payload.data() + 2), name_size);
        const auto partition = read_u32(request.payload, 2U + name_size);
        const auto start = read_u64(request.payload, 6U + name_size);
        const auto maximum = read_u32(request.payload, 14U + name_size);
        if (maximum > 1000) throw std::invalid_argument("fetch count exceeds limit");
        const auto fetched = broker_.fetch(name, partition, start, maximum);
        const auto fetch_dur = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fetch_start).count());
        observability::MetricsRegistry::instance().record_fetch(fetch_dur, fetched.records.size());
        append_u64(response.payload, fetched.next_offset);
        append_u32(response.payload, static_cast<std::uint32_t>(fetched.records.size()));
        for (const auto& record : fetched.records) {
          append_u64(response.payload, record.offset);
          append_u64(response.payload, static_cast<std::uint64_t>(record.timestamp_unix_ms));
          append_u32(response.payload, static_cast<std::uint32_t>(record.key.size()));
          append_u32(response.payload, static_cast<std::uint32_t>(record.value.size()));
          response.payload.insert(response.payload.end(), record.key.begin(), record.key.end());
          response.payload.insert(response.payload.end(), record.value.begin(), record.value.end());
        }
        break;
      }
      case protocol::Operation::kCommitOffsets: {
        if (request.payload.size() < 16) throw std::invalid_argument("truncated offset commit");
        const auto group_size = read_u16(request.payload, 0);
        const auto topic_size_position = static_cast<std::size_t>(2U + group_size);
        if (group_size == 0 || topic_size_position + 2U > request.payload.size()) throw std::invalid_argument("invalid offset group");
        const auto topic_size = read_u16(request.payload, topic_size_position);
        const auto tail = topic_size_position + 2U + topic_size;
        if (topic_size == 0 || request.payload.size() != tail + 12U) throw std::invalid_argument("invalid offset commit length");
        const std::string group(reinterpret_cast<const char*>(request.payload.data() + 2), group_size);
        const std::string topic(reinterpret_cast<const char*>(request.payload.data() + topic_size_position + 2U), topic_size);
        broker_.commit_offset(group, topic, read_u32(request.payload, tail), read_u64(request.payload, tail + 4U));
        break;
      }
      case protocol::Operation::kGetMetrics: {
        const auto metrics = observability::MetricsRegistry::instance().export_prometheus();
        response.payload.assign(reinterpret_cast<const std::byte*>(metrics.data()),
                                reinterpret_cast<const std::byte*>(metrics.data() + metrics.size()));
        break;
      }
      default:
        response.header.status = protocol::Status::kUnknownOperation;
        break;
    }
  } catch (const std::invalid_argument&) {
    response.header.status = protocol::Status::kMalformedFrame;
  } catch (...) {
    response.header.status = protocol::Status::kInternal;
  }
  return response;
}

}  // namespace streamforge::network

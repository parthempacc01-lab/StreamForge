#include "streamforge/benchmark/benchmark.hpp"
#include "streamforge/broker/broker_service.hpp"
#include "streamforge/network/tcp_server.hpp"
#include "streamforge/protocol/frame_codec.hpp"

#include <algorithm>
#include <barrier>
#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {
#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
void close_socket(SocketHandle socket) noexcept { closesocket(socket); }
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
void close_socket(SocketHandle socket) noexcept { close(socket); }
#endif

class Socket {
 public:
  explicit Socket(SocketHandle socket) : socket_(socket) {}
  ~Socket() { if (socket_ != kInvalidSocket) close_socket(socket_); }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept : socket_(std::exchange(other.socket_, kInvalidSocket)) {}
  Socket& operator=(Socket&& other) noexcept {
    if (this != &other) {
      if (socket_ != kInvalidSocket) close_socket(socket_);
      socket_ = std::exchange(other.socket_, kInvalidSocket);
    }
    return *this;
  }
  [[nodiscard]] SocketHandle get() const noexcept { return socket_; }
 private:
  SocketHandle socket_;
};

void append_u16(std::vector<std::byte>& out, std::uint16_t value) { out.push_back(static_cast<std::byte>(value >> 8U)); out.push_back(static_cast<std::byte>(value)); }
void append_u32(std::vector<std::byte>& out, std::uint32_t value) { for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<std::byte>(value >> shift)); }
void append_u64(std::vector<std::byte>& out, std::uint64_t value) { for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<std::byte>(value >> shift)); }

bool send_all(SocketHandle socket, const std::vector<std::byte>& bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
#ifdef _WIN32
    const auto result = send(socket, reinterpret_cast<const char*>(bytes.data() + sent), static_cast<int>(bytes.size() - sent), 0);
#else
    const auto result = send(socket, bytes.data() + sent, bytes.size() - sent, 0);
#endif
    if (result <= 0) return false;
    sent += static_cast<std::size_t>(result);
  }
  return true;
}

streamforge::protocol::Frame receive_frame(SocketHandle socket) {
  streamforge::protocol::FrameDecoder decoder;
  std::vector<std::byte> buffer(8192);
  while (!decoder.has_frame()) {
#ifdef _WIN32
    const auto received = recv(socket, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0);
#else
    const auto received = recv(socket, buffer.data(), buffer.size(), 0);
#endif
    if (received <= 0 || !decoder.feed(std::span{buffer}.first(static_cast<std::size_t>(received)))) throw std::runtime_error("invalid or missing response");
  }
  return decoder.take_frame();
}

Socket connect_loopback(std::uint16_t port) {
  const auto socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket == kInvalidSocket) throw std::runtime_error("cannot create benchmark client socket");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    close_socket(socket);
    throw std::runtime_error("cannot connect benchmark client");
  }
  return Socket(socket);
}

void set_timeouts(SocketHandle socket) {
#ifdef _WIN32
  const DWORD timeout_ms = 10000;
  const auto failed = setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout_ms), sizeof(timeout_ms)) != 0 || setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout_ms), sizeof(timeout_ms)) != 0;
#else
  const timeval timeout{10, 0};
  const auto failed = setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 || setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0;
#endif
  if (failed) throw std::runtime_error("cannot set client timeout");
}

std::vector<std::byte> produce_payload(std::size_t records, std::size_t value_size,
                                       std::uint32_t partition) {
  std::vector<std::byte> out;
  out.reserve(15U + records * (16U + value_size));
  append_u16(out, 9);
  for (const char character : std::string("benchmark")) out.push_back(static_cast<std::byte>(character));
  append_u32(out, partition); out.push_back(std::byte{0}); append_u32(out, static_cast<std::uint32_t>(records));
  for (std::size_t index = 0; index < records; ++index) {
    append_u64(out, 0); append_u32(out, 0); append_u32(out, static_cast<std::uint32_t>(value_size));
    out.insert(out.end(), value_size, std::byte{0x42});
  }
  return out;
}

void request(SocketHandle socket, streamforge::protocol::Operation operation, std::uint64_t request_id, const std::vector<std::byte>& payload) {
  const auto encoded = streamforge::protocol::encode_frame({streamforge::protocol::FrameHeader{.operation = operation, .request_id = request_id}, payload});
  if (!send_all(socket, encoded)) throw std::runtime_error("request send failed");
  const auto response = receive_frame(socket);
  if (response.header.request_id != request_id || response.header.status != streamforge::protocol::Status::kOk) throw std::runtime_error("request returned a protocol error");
}

std::vector<std::byte> create_topic_payload(std::uint32_t partitions) {
  std::vector<std::byte> out;
  append_u16(out, 9);
  for (const char character : std::string("benchmark")) out.push_back(static_cast<std::byte>(character));
  append_u32(out, partitions);
  return out;
}

void validate_options(const streamforge::benchmark::Options& options) {
  if (options.mode != "appended") throw std::invalid_argument("TCP benchmark supports appended mode only");
  if (options.payload_bytes > 1024U * 1024U || options.batch_size > 10000U) throw std::invalid_argument("payload or batch exceeds protocol limits");
  if (options.payload_bytes > (std::numeric_limits<std::size_t>::max() - 15U) / options.batch_size - 16U || 15U + options.batch_size * (16U + options.payload_bytes) > streamforge::protocol::kDefaultMaximumFrameBodySize - streamforge::protocol::kFrameHeaderSize) throw std::invalid_argument("produce batch exceeds frame size limit");
}

class Failures {
 public:
  void capture() { std::lock_guard lock(mutex_); if (!exception_) exception_ = std::current_exception(); }
  void rethrow_if_any() const { std::lock_guard lock(mutex_); if (exception_) std::rethrow_exception(exception_); }
 private:
  mutable std::mutex mutex_;
  std::exception_ptr exception_;
};

struct ClientResult { std::size_t successful_messages{0}; std::size_t failed_requests{0}; std::vector<double> latencies_ms; };
std::string format_counts(const std::vector<std::size_t>& counts) {
  std::ostringstream output;
  for (std::size_t index = 0; index < counts.size(); ++index) {
    if (index != 0) output << ',';
    output << counts[index];
  }
  return output.str();
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const auto options = streamforge::benchmark::parse_options(argc, argv);
    validate_options(options);
    std::vector<double> run_rates, all_latencies_ms;
    std::size_t total_successful = 0, total_failed_requests = 0;
    for (std::size_t run = 0; run < options.repetitions; ++run) {
      const auto expected_per_partition = streamforge::benchmark::messages_per_partition(
          options.messages, options.clients, options.partitions);
      const auto root = std::filesystem::path(options.data_directory) / ("tcp-run-" + std::to_string(run));
      if (std::filesystem::exists(root)) throw std::invalid_argument("benchmark directory already exists; use a new data directory");
      streamforge::broker::BrokerService broker(root); broker.recover();
      // The control connection creates the topic before measurement. Reserve
      // one extra slot so the requested number of measured clients is always
      // admitted even while that connection is being closed.
      streamforge::network::TcpServer server(broker, {.maximum_active_connections = options.clients + 1U}); server.start();
      try {
        auto control = connect_loopback(server.port()); set_timeouts(control.get());
        request(control.get(), streamforge::protocol::Operation::kCreateTopic, 1,
                create_topic_payload(static_cast<std::uint32_t>(options.partitions)));
        std::vector<Socket> sockets; sockets.reserve(options.clients);
        for (std::size_t client = 0; client < options.clients; ++client) { sockets.emplace_back(connect_loopback(server.port())); set_timeouts(sockets.back().get()); }
        std::barrier ready(static_cast<std::ptrdiff_t>(options.clients + 1));
        std::barrier start(static_cast<std::ptrdiff_t>(options.clients + 1));
        Failures failures; std::vector<ClientResult> results(options.clients); std::vector<std::thread> workers; workers.reserve(options.clients);
        for (std::size_t client = 0; client < options.clients; ++client) {
          const std::size_t client_messages = options.messages / options.clients + (client < options.messages % options.clients ? 1U : 0U);
          const auto partition = streamforge::benchmark::target_partition(client, options.partitions);
          workers.emplace_back([&, client, client_messages, partition] {
            try {
              const auto full_payload = produce_payload(options.batch_size, options.payload_bytes, partition);
              for (std::size_t warmup = 0; warmup < options.warmup_batches; ++warmup) request(sockets[client].get(), streamforge::protocol::Operation::kProduceBatch, static_cast<std::uint64_t>(10'000'000ULL + client * 100'000ULL + warmup), full_payload);
            } catch (...) { ++results[client].failed_requests; failures.capture(); }
            ready.arrive_and_wait();
            start.arrive_and_wait();
            try {
              std::size_t completed = 0; const auto full_payload = produce_payload(options.batch_size, options.payload_bytes, partition);
              while (completed < client_messages) {
                const auto batch = std::min(options.batch_size, client_messages - completed);
                std::vector<std::byte> partial_payload;
                const auto* payload = &full_payload;
                if (batch != options.batch_size) { partial_payload = produce_payload(batch, options.payload_bytes, partition); payload = &partial_payload; }
                const auto before = std::chrono::steady_clock::now();
                request(sockets[client].get(), streamforge::protocol::Operation::kProduceBatch, static_cast<std::uint64_t>(20'000'000ULL + client * 1'000'000ULL + completed), *payload);
                results[client].latencies_ms.push_back(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - before).count());
                completed += batch; results[client].successful_messages += batch;
              }
            } catch (...) { ++results[client].failed_requests; failures.capture(); }
          });
        }
        ready.arrive_and_wait();
        if (options.profile) { broker.set_profiling_enabled(true); server.set_profiling_enabled(true); }
        const auto started = std::chrono::steady_clock::now(); start.arrive_and_wait();
        for (auto& worker : workers) worker.join();
        const auto elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        failures.rethrow_if_any();
        std::size_t successful = 0, failed = 0; std::vector<double> latencies_ms;
        std::vector<std::size_t> successful_per_partition(options.partitions);
        for (std::size_t client = 0; client < results.size(); ++client) { auto& result = results[client]; successful += result.successful_messages; failed += result.failed_requests; successful_per_partition[streamforge::benchmark::target_partition(client, options.partitions)] += result.successful_messages; latencies_ms.insert(latencies_ms.end(), result.latencies_ms.begin(), result.latencies_ms.end()); }
        std::vector<std::size_t> stored_per_partition(options.partitions);
        for (std::size_t partition = 0; partition < options.partitions; ++partition) {
          std::uint64_t next_offset = 0;
          while (stored_per_partition[partition] < expected_per_partition[partition]) {
            const auto fetched = broker.fetch("benchmark", static_cast<std::uint32_t>(partition),
                                              next_offset, std::min<std::size_t>(1000U, expected_per_partition[partition] - stored_per_partition[partition]));
            if (fetched.records.empty()) throw std::runtime_error("benchmark partition storage count mismatch");
            stored_per_partition[partition] += fetched.records.size();
            next_offset = fetched.next_offset;
          }
        }
        if (failed != 0 || successful != options.messages || successful_per_partition != expected_per_partition || stored_per_partition != expected_per_partition) throw std::runtime_error("benchmark message count mismatch");
        const auto messages_per_second = static_cast<double>(successful) / elapsed_seconds;
        const auto mib_per_second = messages_per_second * static_cast<double>(options.payload_bytes) / (1024.0 * 1024.0);
        std::cout << "run=" << run + 1 << " clients=" << options.clients << " partitions=" << options.partitions << " partition_mapping=client_index_mod_partitions profile=" << (options.profile ? "on" : "off") << " expected_messages_per_partition=" << format_counts(expected_per_partition) << " successful_messages_per_partition=" << format_counts(successful_per_partition) << " stored_messages_per_partition=" << format_counts(stored_per_partition) << " successful_messages=" << successful << " failed_requests=" << failed << " elapsed_seconds=" << elapsed_seconds << " messages_per_second=" << messages_per_second << " mib_per_second=" << mib_per_second << '\n';
        if (options.profile) { const auto tcp = server.profile(); const auto service = broker.profile(); std::cout << "profile_run=" << run + 1 << " tcp_dispatch_operations=" << tcp.operations << " tcp_processing_ms=" << tcp.processing_ns / 1e6 << " broker_operations=" << service.operations << " partition_wait_ms=" << service.partition_wait_ns / 1e6 << " partition_hold_ms=" << service.partition_hold_ns / 1e6 << " append_ms=" << service.append_ns / 1e6 << " stream_flush_ms=" << service.flush_ns / 1e6 << '\n'; }
        total_successful += successful; total_failed_requests += failed; run_rates.push_back(messages_per_second); all_latencies_ms.insert(all_latencies_ms.end(), latencies_ms.begin(), latencies_ms.end());
      } catch (...) { server.stop(); throw; }
      server.stop();
    }
    const auto summary = streamforge::benchmark::summarize(all_latencies_ms);
    const auto median_rate = streamforge::benchmark::percentile(run_rates, .50);
    std::cout << "benchmark=tcp_produce clients=" << options.clients << " partitions=" << options.partitions << " partition_mapping=client_index_mod_partitions profile=" << (options.profile ? "on" : "off") << " outstanding_requests_per_client=1 successful_messages=" << total_successful << " failed_requests=" << total_failed_requests << " messages_per_run=" << options.messages << " payload_bytes=" << options.payload_bytes << " batch_size=" << options.batch_size << " persistence_mode=appended timing=from_before_send_through_complete_response_validation_excluding_server_start_topic_creation_connection_setup_and_warmup latency_samples=" << all_latencies_ms.size() << " percentile_method=pooled_samples_sorted_floor_p_times_n_minus_1 p50_request_ms=" << summary.p50_ms << " p95_request_ms=" << summary.p95_ms << " p99_request_ms=" << summary.p99_ms << " median_messages_per_second=" << median_rate << " median_mib_per_second=" << median_rate * static_cast<double>(options.payload_bytes) / (1024.0 * 1024.0) << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "TCP benchmark error: " << error.what() << "\nUsage: eventstream_tcp_benchmark <new-data-directory> [--messages N] [--payload-bytes N] [--batch-size N] [--clients 1|2|4|8|16] [--partitions 1|4] [--warmup-batches N] [--repetitions N] [--profile on|off] [--mode appended]\n";
    return 2;
  }
}

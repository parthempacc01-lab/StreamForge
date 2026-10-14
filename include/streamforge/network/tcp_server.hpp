#pragma once

#include "streamforge/broker/broker_service.hpp"
#include "streamforge/protocol/frame_codec.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

namespace streamforge::network {

struct TcpDispatchProfile { std::uint64_t operations{0}; std::uint64_t processing_ns{0}; };

// A deliberately small TCP adapter for the single-node broker. It owns no
// broker data: it turns trusted protocol frames into synchronous service calls.
// start() and stop() serialize lifecycle changes; concurrent lifecycle calls
// are safe, but a start during shutdown is rejected.
class TcpServer {
 public:
  struct Configuration {
    std::uint16_t port{0};  // Zero asks the operating system for a free port.
    std::size_t maximum_frame_body_size{protocol::kDefaultMaximumFrameBodySize};
    std::size_t read_buffer_size{8192};
    // Bounds one-thread-per-connection resource use. Excess connections are
    // accepted and immediately closed without receiving a protocol response.
    std::size_t maximum_active_connections{64};
  };

  explicit TcpServer(broker::BrokerService& broker, Configuration configuration = {});
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  void start();
  void stop() noexcept;
  [[nodiscard]] std::uint16_t port() const noexcept;
  void set_profiling_enabled(bool enabled) noexcept;
  [[nodiscard]] TcpDispatchProfile profile() const noexcept;

 private:
  struct ClientConnection {
    std::intptr_t socket{-1};
    std::thread thread;
  };

  void accept_loop();
  void client_loop(std::intptr_t socket) noexcept;
  void reap_finished_clients();
  [[nodiscard]] protocol::Frame dispatch(const protocol::Frame& request);

  broker::BrokerService& broker_;
  Configuration configuration_;
  std::atomic_bool running_{false};
  mutable std::mutex lifecycle_mutex_;
  bool stopping_{false};
  std::intptr_t listen_socket_{-1};
  std::uint16_t bound_port_{0};
  std::thread accept_thread_;
  std::vector<ClientConnection> client_connections_;
  std::mutex sockets_mutex_;
  std::atomic_bool profiling_enabled_{false};
  std::atomic_uint64_t profile_operations_{0}, profile_processing_ns_{0};
};

}  // namespace streamforge::network

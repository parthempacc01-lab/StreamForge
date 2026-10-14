#include "streamforge/broker/broker_service.hpp"
#include "streamforge/network/tcp_server.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>
namespace { std::atomic_bool stop_requested{false}; void stop(int) { stop_requested = true; } }
int main(int argc, char** argv) {
  if (argc != 3) { std::cerr << "Usage: eventstream_server <data-directory> <port>\n"; return 2; }
  try { streamforge::broker::BrokerService broker(argv[1]); broker.recover();
    streamforge::network::TcpServer server(broker, {.port=static_cast<std::uint16_t>(std::strtoul(argv[2], nullptr, 10))});
    std::signal(SIGINT, stop); std::signal(SIGTERM, stop); server.start();
    std::cout << "EventStream listening on port " << server.port() << "\n";
    while (!stop_requested) std::this_thread::sleep_for(std::chrono::milliseconds(100)); server.stop(); return 0;
  } catch (const std::exception& error) { std::cerr << "server error: " << error.what() << '\n'; return 1; }
}

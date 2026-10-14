#include "streamforge/protocol/frame_codec.hpp"
#include <array>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
int main(int argc, char** argv) {
  if (argc != 4 || std::string(argv[3]) != "ping") { std::cerr << "Usage: eventstream_client <host> <port> ping\n"; return 2; }
  try {
#ifdef _WIN32
    WSADATA data{}; if (WSAStartup(MAKEWORD(2,2), &data) != 0) throw std::runtime_error("WSAStartup failed");
#endif
    const auto socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket < 0) throw std::runtime_error("cannot create socket");
    sockaddr_in address{}; address.sin_family=AF_INET; address.sin_port=htons(static_cast<u_short>(std::stoul(argv[2])));
    if (inet_pton(AF_INET, argv[1], &address.sin_addr) != 1 || connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) throw std::runtime_error("cannot connect");
    const auto bytes=streamforge::protocol::encode_frame({streamforge::protocol::FrameHeader{.operation=streamforge::protocol::Operation::kPing,.request_id=1},{}});
    if (send(socket, reinterpret_cast<const char*>(bytes.data()), static_cast<int>(bytes.size()), 0) != static_cast<int>(bytes.size())) throw std::runtime_error("cannot send request");
    std::array<std::byte, 256> buffer{}; const auto received=recv(socket,reinterpret_cast<char*>(buffer.data()),static_cast<int>(buffer.size()),0); if (received <= 0) throw std::runtime_error("no response");
    streamforge::protocol::FrameDecoder decoder; if (!decoder.feed(std::span{buffer}.first(static_cast<std::size_t>(received))) || !decoder.has_frame()) throw std::runtime_error("invalid response");
    const auto response=decoder.take_frame(); std::cout << "status=" << static_cast<std::uint16_t>(response.header.status) << " request_id=" << response.header.request_id << '\n';
#ifdef _WIN32
    closesocket(socket); WSACleanup();
#else
    close(socket);
#endif
    return response.header.status == streamforge::protocol::Status::kOk ? 0 : 1;
  } catch (const std::exception& error) { std::cerr << "client error: " << error.what() << '\n'; return 1; }
}

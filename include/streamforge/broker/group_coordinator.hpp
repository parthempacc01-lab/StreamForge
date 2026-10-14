#pragma once
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>
namespace streamforge::broker {
struct GroupAssignment { std::uint64_t generation; std::vector<std::uint32_t> partitions; };
class GroupCoordinator {
 public:
  GroupAssignment join(const std::string& group, const std::string& member, std::uint32_t partitions);
  GroupAssignment heartbeat(const std::string& group, const std::string& member);
  void leave(const std::string& group, const std::string& member);
  void expire(std::chrono::steady_clock::time_point now, std::chrono::milliseconds timeout);
 private:
  struct Member { std::chrono::steady_clock::time_point seen; };
  struct Group { std::uint64_t generation{0}; std::uint32_t partitions{0}; std::map<std::string, Member> members; };
  std::map<std::string, Group> groups_;
  [[nodiscard]] GroupAssignment assignment(const Group& group, const std::string& member) const;
};
}  // namespace streamforge::broker

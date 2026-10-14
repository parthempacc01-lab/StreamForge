#include "streamforge/broker/group_coordinator.hpp"
#include <iterator>
#include <stdexcept>
namespace streamforge::broker {
GroupAssignment GroupCoordinator::assignment(const Group& group, const std::string& member) const {
  const auto position = static_cast<std::uint32_t>(std::distance(group.members.begin(), group.members.find(member)));
  GroupAssignment result{group.generation, {}}; const auto members = static_cast<std::uint32_t>(group.members.size());
  for (std::uint32_t p = position; p < group.partitions; p += members) result.partitions.push_back(p);
  return result;
}
GroupAssignment GroupCoordinator::join(const std::string& group_name, const std::string& member, std::uint32_t partitions) {
  if (group_name.empty() || member.empty() || partitions == 0) throw std::invalid_argument("invalid group join");
  auto& group = groups_[group_name];
  if (group.partitions != 0 && group.partitions != partitions) throw std::invalid_argument("group topic partition count changed");
  group.partitions = partitions; const auto [_, inserted] = group.members.emplace(member, Member{std::chrono::steady_clock::now()});
  if (inserted) ++group.generation; else group.members[member].seen = std::chrono::steady_clock::now();
  return assignment(group, member);
}
GroupAssignment GroupCoordinator::heartbeat(const std::string& group_name, const std::string& member) {
  const auto group = groups_.find(group_name); if (group == groups_.end() || !group->second.members.contains(member)) throw std::out_of_range("unknown group member");
  group->second.members[member].seen = std::chrono::steady_clock::now(); return assignment(group->second, member);
}
void GroupCoordinator::leave(const std::string& name, const std::string& member) { auto group = groups_.find(name); if (group == groups_.end()) return; if (group->second.members.erase(member)) ++group->second.generation; if (group->second.members.empty()) groups_.erase(group); }
void GroupCoordinator::expire(std::chrono::steady_clock::time_point now, std::chrono::milliseconds timeout) { for (auto group = groups_.begin(); group != groups_.end();) { auto& members = group->second.members; for (auto it = members.begin(); it != members.end();) { if (now - it->second.seen > timeout) { it = members.erase(it); ++group->second.generation; } else ++it; } if (members.empty()) group = groups_.erase(group); else ++group; } }
}  // namespace streamforge::broker

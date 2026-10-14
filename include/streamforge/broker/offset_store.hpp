#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <tuple>

namespace streamforge::broker {

// Durable consumer progress metadata. Each commit replaces the prior position
// for one group/topic/partition tuple.
class OffsetStore {
 public:
  explicit OffsetStore(std::filesystem::path data_root);
  void load();
  void commit(const std::string& group, const std::string& topic, std::uint32_t partition,
              std::uint64_t next_offset);
  [[nodiscard]] std::optional<std::uint64_t> get(const std::string& group,
                                                  const std::string& topic,
                                                  std::uint32_t partition) const;
 private:
  void persist() const;
  [[nodiscard]] std::filesystem::path path() const;
  std::filesystem::path data_root_;
  std::map<std::tuple<std::string, std::string, std::uint32_t>, std::uint64_t> offsets_;
};

}  // namespace streamforge::broker

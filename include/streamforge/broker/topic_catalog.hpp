#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>

namespace streamforge::broker {

struct TopicDescription {
  std::string name;
  std::uint32_t partition_count;
};

// Versioned durable catalog. The in-memory map is authoritative only after
// load() has validated the on-disk generation.
class TopicCatalog {
 public:
  explicit TopicCatalog(std::filesystem::path data_root);

  void load();
  void create_topic(std::string name, std::uint32_t partition_count);
  [[nodiscard]] TopicDescription describe(const std::string& name) const;
  [[nodiscard]] const std::map<std::string, TopicDescription>& topics() const noexcept;

 private:
  void persist() const;
  [[nodiscard]] std::filesystem::path metadata_directory() const;
  [[nodiscard]] std::filesystem::path catalog_path() const;

  std::filesystem::path data_root_;
  std::map<std::string, TopicDescription> topics_;
};

}  // namespace streamforge::broker

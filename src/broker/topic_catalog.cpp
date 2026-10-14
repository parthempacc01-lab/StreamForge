#include "streamforge/broker/topic_catalog.hpp"

#include <cctype>
#include <fstream>
#include <stdexcept>
#include <utility>

namespace streamforge::broker {
namespace {

constexpr const char* kCatalogMagic = "STREAMFORGE_TOPICS_V1";

bool valid_topic_name(const std::string& name) {
  if (name.empty() || name.size() > 249 || name == "." || name == "..") return false;
  for (const char ch : name) {
    const auto character = static_cast<unsigned char>(ch);
    if (!(std::isalnum(character) || character == '.' || character == '_' || character == '-')) {
      return false;
    }
  }
  return true;
}

}  // namespace

TopicCatalog::TopicCatalog(std::filesystem::path data_root) : data_root_(std::move(data_root)) {}

void TopicCatalog::load() {
  std::filesystem::create_directories(metadata_directory());
  if (!std::filesystem::exists(catalog_path())) return;
  std::ifstream input(catalog_path());
  std::string magic;
  if (!std::getline(input, magic) || magic != kCatalogMagic) {
    throw std::runtime_error("topic catalog has an unsupported or invalid format");
  }
  std::map<std::string, TopicDescription> loaded;
  std::string name;
  std::uint32_t partitions = 0;
  while (input >> name >> partitions) {
    if (!valid_topic_name(name) || partitions == 0 || loaded.contains(name)) {
      throw std::runtime_error("topic catalog contains an invalid topic entry");
    }
    loaded.emplace(name, TopicDescription{name, partitions});
  }
  if (!input.eof()) throw std::runtime_error("topic catalog ended with malformed data");
  topics_ = std::move(loaded);
}

void TopicCatalog::create_topic(std::string name, std::uint32_t partition_count) {
  if (!valid_topic_name(name) || partition_count == 0) {
    throw std::invalid_argument("invalid topic name or partition count");
  }
  if (topics_.contains(name)) throw std::invalid_argument("topic already exists");
  topics_.emplace(name, TopicDescription{name, partition_count});
  try {
    persist();
  } catch (...) {
    topics_.erase(name);
    throw;
  }
}

TopicDescription TopicCatalog::describe(const std::string& name) const {
  const auto topic = topics_.find(name);
  if (topic == topics_.end()) throw std::out_of_range("unknown topic");
  return topic->second;
}

const std::map<std::string, TopicDescription>& TopicCatalog::topics() const noexcept { return topics_; }

void TopicCatalog::persist() const {
  std::filesystem::create_directories(metadata_directory());
  const auto temporary = catalog_path().string() + ".tmp";
  {
    std::ofstream output(temporary, std::ios::trunc);
    if (!output) throw std::runtime_error("cannot create temporary topic catalog");
    output << kCatalogMagic << '\n';
    for (const auto& [name, topic] : topics_) output << name << ' ' << topic.partition_count << '\n';
    output.flush();
    if (!output) throw std::runtime_error("cannot write complete topic catalog");
  }
  std::error_code error;
  std::filesystem::rename(temporary, catalog_path(), error);
  if (error) {
    std::filesystem::remove(temporary);
    throw std::runtime_error("cannot atomically replace topic catalog: " + error.message());
  }
}

std::filesystem::path TopicCatalog::metadata_directory() const { return data_root_ / "metadata"; }
std::filesystem::path TopicCatalog::catalog_path() const { return metadata_directory() / "topics"; }

}  // namespace streamforge::broker

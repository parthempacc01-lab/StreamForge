#include "streamforge/broker/offset_store.hpp"
#include "streamforge/storage/durability.hpp"

#include <fstream>
#include <stdexcept>
#include <utility>

namespace streamforge::broker {
namespace { constexpr const char* kMagic = "STREAMFORGE_OFFSETS_V1"; }

OffsetStore::OffsetStore(std::filesystem::path data_root) : data_root_(std::move(data_root)) {}
std::filesystem::path OffsetStore::path() const { return data_root_ / "metadata" / "offsets"; }

void OffsetStore::load() {
  std::filesystem::create_directories(path().parent_path());
  if (!std::filesystem::exists(path())) return;
  std::ifstream input(path()); std::string magic;
  if (!std::getline(input, magic) || magic != kMagic) throw std::runtime_error("invalid offset store");
  decltype(offsets_) loaded; std::string group, topic; std::uint32_t partition; std::uint64_t offset;
  while (input >> group >> topic >> partition >> offset) {
    if (group.empty() || topic.empty() || !loaded.emplace(std::make_tuple(group, topic, partition), offset).second)
      throw std::runtime_error("invalid offset entry");
  }
  if (!input.eof()) throw std::runtime_error("malformed offset store");
  offsets_ = std::move(loaded);
}

void OffsetStore::commit(const std::string& group, const std::string& topic, std::uint32_t partition,
                         std::uint64_t next_offset) {
  if (group.empty() || group.find_first_of(" \t\r\n") != std::string::npos) throw std::invalid_argument("invalid group name");
  const auto key = std::make_tuple(group, topic, partition);
  const auto previous = offsets_.find(key);
  const auto saved = previous == offsets_.end() ? std::optional<std::uint64_t>{} : std::optional{previous->second};
  offsets_[key] = next_offset;
  try { persist(); } catch (...) { if (saved) offsets_[key] = *saved; else offsets_.erase(key); throw; }
}

std::optional<std::uint64_t> OffsetStore::get(const std::string& group, const std::string& topic, std::uint32_t partition) const {
  const auto found = offsets_.find(std::make_tuple(group, topic, partition));
  return found == offsets_.end() ? std::nullopt : std::optional{found->second};
}

void OffsetStore::persist() const {
  std::filesystem::create_directories(path().parent_path());
  const auto temporary = path().string() + ".tmp";
  {
    std::ofstream output(temporary, std::ios::trunc);
    if (!output) throw std::runtime_error("cannot write offset store");
    output << kMagic << '\n';
    for (const auto& [key, offset] : offsets_) {
      const auto& [group, topic, partition] = key;
      output << group << ' ' << topic << ' ' << partition << ' ' << offset << '\n';
    }
    output.flush();
    if (!output) throw std::runtime_error("cannot complete offset store");
  }
  storage::atomic_durable_rename(temporary, path());
}
}  // namespace streamforge::broker

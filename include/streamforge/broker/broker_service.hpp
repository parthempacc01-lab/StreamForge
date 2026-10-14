#pragma once

#include "streamforge/broker/topic_catalog.hpp"
#include "streamforge/broker/offset_store.hpp"
#include "streamforge/storage/segment_log.hpp"

#include <cstddef>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace streamforge::broker {

struct FetchedRecords {
  std::vector<storage::Record> records;
  std::uint64_t next_offset;
};
struct BrokerLimits { std::uint32_t maximum_partitions_per_topic{1024}; std::size_t maximum_records_per_batch{10000}; std::size_t maximum_fetch_records{1000}; };
struct BrokerProfile { std::uint64_t operations{0}; std::uint64_t partition_wait_ns{0}; std::uint64_t partition_hold_ns{0}; std::uint64_t append_ns{0}; std::uint64_t flush_ns{0}; };

// Single-process service façade. Public operations take a shared lifecycle lock;
// recovery takes it exclusively. The catalog/log map is protected separately,
// and each retained PartitionState serializes its own SegmentLog. A partition
// state is never removed, so callers may release the catalog lock before using
// its log. TopicCatalog, OffsetStore, and SegmentLog remain non-thread-safe
// implementation details and must only be accessed through these locks.
class BrokerService {
 public:
  explicit BrokerService(std::filesystem::path data_root, BrokerLimits limits = {});

  void recover();
  void create_topic(const std::string& name, std::uint32_t partition_count);
  [[nodiscard]] storage::AppendResult produce(
      const std::string& topic, std::uint32_t partition,
      std::vector<storage::Record> records, storage::AcknowledgementMode mode);
  [[nodiscard]] FetchedRecords fetch(const std::string& topic, std::uint32_t partition,
                                     std::uint64_t start_offset,
                                     std::size_t maximum_records);
  void commit_offset(const std::string& group, const std::string& topic, std::uint32_t partition,
                     std::uint64_t next_offset);
  [[nodiscard]] std::optional<std::uint64_t> committed_offset(const std::string& group,
                                                               const std::string& topic,
                                                               std::uint32_t partition) const;
  void set_profiling_enabled(bool enabled) noexcept;
  [[nodiscard]] BrokerProfile profile() const noexcept;

 private:
  struct PartitionState {
    explicit PartitionState(std::unique_ptr<storage::SegmentLog> value) : log(std::move(value)) {}
    std::mutex mutex;
    std::unique_ptr<storage::SegmentLog> log;
  };

  [[nodiscard]] std::shared_ptr<PartitionState> partition(const std::string& topic,
                                                            std::uint32_t partition);
  [[nodiscard]] std::shared_ptr<PartitionState> create_partition_locked(
      const std::string& topic, std::uint32_t partition);
  void begin_operation() const noexcept;

  std::filesystem::path data_root_;
  TopicCatalog catalog_;
  OffsetStore offsets_;
  BrokerLimits limits_;
  std::map<std::pair<std::string, std::uint32_t>, std::shared_ptr<PartitionState>> logs_;
  // Lock order: lifecycle -> catalog. Never hold catalog with a partition or
  // offset lock, and never hold a partition and offset lock together.
  mutable std::shared_mutex lifecycle_mutex_;
  mutable std::shared_mutex catalog_mutex_;
  mutable std::mutex offset_mutex_;
  mutable std::atomic_bool operation_started_{false};
  std::atomic_bool profiling_enabled_{false};
  std::atomic_uint64_t profile_operations_{0}, profile_partition_wait_ns_{0}, profile_partition_hold_ns_{0}, profile_append_ns_{0}, profile_flush_ns_{0};
};

}  // namespace streamforge::broker

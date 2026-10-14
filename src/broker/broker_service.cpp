#include "streamforge/broker/broker_service.hpp"

#include <stdexcept>
#include <chrono>
#include <utility>

namespace streamforge::broker {

BrokerService::BrokerService(std::filesystem::path data_root, BrokerLimits limits)
    : data_root_(std::move(data_root)), catalog_(data_root_), offsets_(data_root_), limits_(limits) {}

void BrokerService::recover() {
  std::unique_lock lifecycle_lock(lifecycle_mutex_);
  if (operation_started_.load(std::memory_order_acquire)) throw std::logic_error("recover is only valid during broker startup");
  std::unique_lock catalog_lock(catalog_mutex_);
  std::lock_guard offset_lock(offset_mutex_);
  catalog_.load();
  offsets_.load();
  logs_.clear();
  for (const auto& [name, topic] : catalog_.topics()) {
    for (std::uint32_t partition_id = 0; partition_id < topic.partition_count; ++partition_id) {
      (void)create_partition_locked(name, partition_id);
    }
  }
}

void BrokerService::commit_offset(const std::string& group, const std::string& topic,
                                  std::uint32_t partition_id, std::uint64_t next_offset) {
  std::shared_lock lifecycle_lock(lifecycle_mutex_);
  begin_operation();
  (void)partition(topic, partition_id);
  std::lock_guard offset_lock(offset_mutex_);
  offsets_.commit(group, topic, partition_id, next_offset);
}

std::optional<std::uint64_t> BrokerService::committed_offset(const std::string& group,
                                                              const std::string& topic,
                                                              std::uint32_t partition_id) const {
  std::shared_lock lifecycle_lock(lifecycle_mutex_);
  begin_operation();
  std::lock_guard offset_lock(offset_mutex_);
  return offsets_.get(group, topic, partition_id);
}

void BrokerService::create_topic(const std::string& name, std::uint32_t partition_count) {
  std::shared_lock lifecycle_lock(lifecycle_mutex_);
  begin_operation();
  if (partition_count > limits_.maximum_partitions_per_topic) throw std::invalid_argument("topic exceeds partition limit");
  std::unique_lock catalog_lock(catalog_mutex_);
  catalog_.create_topic(name, partition_count);
  for (std::uint32_t partition_id = 0; partition_id < partition_count; ++partition_id) {
    (void)create_partition_locked(name, partition_id);
  }
}

storage::AppendResult BrokerService::produce(const std::string& topic, std::uint32_t partition_id,
                                             std::vector<storage::Record> records,
                                             storage::AcknowledgementMode mode) {
  std::shared_lock lifecycle_lock(lifecycle_mutex_);
  begin_operation();
  if (records.empty() || records.size() > limits_.maximum_records_per_batch) throw std::invalid_argument("invalid batch size");
  const auto state = partition(topic, partition_id);
  const auto wait_started = std::chrono::steady_clock::now();
  std::unique_lock partition_lock(state->mutex);
  const auto hold_started = std::chrono::steady_clock::now();
  storage::AppendTiming timing{};
  const auto result = state->log->append(std::move(records), mode,
      profiling_enabled_ ? &timing : nullptr);
  // Enforce size-based retention on every produce so the log does not grow
  // unboundedly when a retention_bytes limit is configured.
  (void)state->log->enforce_retention();
  if (profiling_enabled_) {
    profile_operations_.fetch_add(1, std::memory_order_relaxed);
    profile_partition_wait_ns_.fetch_add(static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(hold_started - wait_started).count()), std::memory_order_relaxed);
    profile_partition_hold_ns_.fetch_add(static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - hold_started).count()), std::memory_order_relaxed);
    profile_append_ns_.fetch_add(timing.append_ns, std::memory_order_relaxed);
    profile_flush_ns_.fetch_add(timing.flush_ns, std::memory_order_relaxed);
  }
  return result;
}

void BrokerService::set_profiling_enabled(bool enabled) noexcept {
  profile_operations_ = 0; profile_partition_wait_ns_ = 0; profile_partition_hold_ns_ = 0;
  profile_append_ns_ = 0; profile_flush_ns_ = 0; profiling_enabled_ = enabled;
}

BrokerProfile BrokerService::profile() const noexcept {
  return {profile_operations_.load(), profile_partition_wait_ns_.load(), profile_partition_hold_ns_.load(), profile_append_ns_.load(), profile_flush_ns_.load()};
}

FetchedRecords BrokerService::fetch(const std::string& topic, std::uint32_t partition_id,
                                    std::uint64_t start_offset, std::size_t maximum_records) {
  std::shared_lock lifecycle_lock(lifecycle_mutex_);
  begin_operation();
  if (maximum_records > limits_.maximum_fetch_records) throw std::invalid_argument("fetch exceeds record limit");
  const auto state = partition(topic, partition_id);
  std::lock_guard partition_lock(state->mutex);
  auto records = state->log->fetch(start_offset, maximum_records);
  const auto next_offset = records.empty() ? start_offset : records.back().offset + 1;
  return FetchedRecords{std::move(records), next_offset};
}

void BrokerService::begin_operation() const noexcept { operation_started_.store(true, std::memory_order_release); }

std::shared_ptr<BrokerService::PartitionState> BrokerService::partition(const std::string& topic,
                                                                         std::uint32_t partition_id) {
  std::shared_lock catalog_lock(catalog_mutex_);
  const auto description = catalog_.describe(topic);
  if (partition_id >= description.partition_count) throw std::out_of_range("invalid partition");
  const auto key = std::make_pair(topic, partition_id);
  const auto found = logs_.find(key);
  if (found == logs_.end()) throw std::logic_error("partition log is not initialized");
  return found->second;
}

std::shared_ptr<BrokerService::PartitionState> BrokerService::create_partition_locked(
    const std::string& topic, std::uint32_t partition_id) {
  const auto key = std::make_pair(topic, partition_id);
  if (const auto found = logs_.find(key); found != logs_.end()) return found->second;
  const auto directory = data_root_ / "topics" / topic /
                         ("partition-" + std::to_string(partition_id));
  auto log = std::make_unique<storage::SegmentLog>(directory, 0);
  log->recover();
  auto state = std::make_shared<PartitionState>(std::move(log));
  auto [inserted, unused] = logs_.emplace(key, std::move(state));
  (void)unused;
  return inserted->second;
}

}  // namespace streamforge::broker

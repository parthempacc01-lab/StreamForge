#pragma once

#include "streamforge/storage/record_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace streamforge::storage {

enum class AcknowledgementMode { kAppended, kSynced };

struct AppendResult {
  std::uint64_t first_offset;
  std::uint64_t last_offset;
};
struct AppendTiming { std::uint64_t append_ns{0}; std::uint64_t flush_ns{0}; };
struct SegmentLogOptions {
  std::size_t maximum_record_body_size{kDefaultMaximumRecordBodySize};
  // A batch is never split across segments. An otherwise empty segment may
  // exceed this limit when one encoded batch itself is larger.
  std::size_t maximum_segment_bytes{128U * 1024U * 1024U};
  // Maximum total retained bytes across all closed and active segments.
  // 0 means retention by size is disabled.
  std::size_t retention_bytes{0};
  // Fault-injection testing hook: if non-zero, simulates a write failure
  // after writing N records in an append batch (0 disables fault injection).
  std::size_t inject_write_failure_after_records{0};
};

struct RetentionStats {
  std::size_t segments_deleted{0};
  std::size_t bytes_reclaimed{0};
};

// Partition-local durable ordered log. BrokerService serializes all calls for
// one partition; this class intentionally has no internal synchronization.
class SegmentLog {
 public:
  struct IndexEntry {
    std::uint64_t offset;
    std::uint64_t position;
  };

  SegmentLog(std::filesystem::path directory, std::uint64_t base_offset,
             std::size_t maximum_record_body_size = kDefaultMaximumRecordBodySize);
  SegmentLog(std::filesystem::path directory, std::uint64_t base_offset,
             SegmentLogOptions options);

  void recover();
  [[nodiscard]] AppendResult append(std::vector<Record> records,
                                    AcknowledgementMode mode, AppendTiming* timing = nullptr);
  [[nodiscard]] std::vector<Record> fetch(std::uint64_t start_offset,
                                          std::size_t maximum_records) const;
  [[nodiscard]] RetentionStats enforce_retention();
  [[nodiscard]] std::uint64_t next_offset() const noexcept;
  [[nodiscard]] std::uint64_t base_offset() const noexcept;
  [[nodiscard]] const std::filesystem::path& path() const noexcept;
  [[nodiscard]] std::size_t segment_count() const noexcept;
  [[nodiscard]] std::size_t total_bytes() const noexcept;
  [[nodiscard]] bool needs_recovery() const noexcept;
  void set_fault_injection_after_records(std::size_t count) noexcept;

 private:
  void synchronize() const;
  struct Segment {
    std::uint64_t base_offset;
    std::uint64_t next_offset;
    std::filesystem::path path;
    std::filesystem::path index_path;
    std::size_t bytes;
    std::vector<IndexEntry> index;
  };
  [[nodiscard]] std::vector<Record> scan_or_throw(const Segment& segment,
                                                   std::uint64_t start_offset,
                                                   std::size_t maximum_records) const;
  void roll_if_needed(std::size_t encoded_batch_bytes);
  void build_or_load_index(Segment& segment);

  std::filesystem::path directory_;
  std::uint64_t base_offset_;
  std::uint64_t next_offset_;
  SegmentLogOptions options_;
  std::vector<Segment> segments_;
  bool needs_recovery_{false};
};

}  // namespace streamforge::storage

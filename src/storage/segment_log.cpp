#include "streamforge/storage/segment_log.hpp"

#include <fstream>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <utility>

#ifdef __unix__
#include <fcntl.h>
#include <unistd.h>
#endif

namespace streamforge::storage {
namespace {

std::filesystem::path segment_path(const std::filesystem::path& directory,
                                   std::uint64_t base_offset) {
  std::ostringstream name;
  name << "segment-" << std::setw(20) << std::setfill('0') << base_offset << ".log";
  return directory / name.str();
}

std::filesystem::path index_path_for(const std::filesystem::path& log_path) {
  auto p = log_path;
  p.replace_extension(".index");
  return p;
}

std::optional<std::uint64_t> parse_segment_base(const std::filesystem::path& path) {
  const auto name = path.filename().string();
  constexpr std::string_view prefix{"segment-"};
  constexpr std::string_view suffix{".log"};
  if (!name.starts_with(prefix) || !name.ends_with(suffix) ||
      name.size() != prefix.size() + 20U + suffix.size()) return std::nullopt;
  const auto digits = std::string_view{name}.substr(prefix.size(), 20U);
  if (!std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; })) return std::nullopt;
  try { return std::stoull(std::string(digits)); } catch (...) { return std::nullopt; }
}

std::vector<std::byte> read_bytes(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  if (!input) {
    throw std::runtime_error("cannot open segment for reading: " + path.string());
  }
  const auto size = input.tellg();
  if (size < 0) {
    throw std::runtime_error("cannot determine segment size: " + path.string());
  }
  std::vector<std::byte> bytes(static_cast<std::size_t>(size));
  input.seekg(0);
  if (!bytes.empty() && !input.read(reinterpret_cast<char*>(bytes.data()), size)) {
    throw std::runtime_error("cannot read complete segment: " + path.string());
  }
  return bytes;
}

void write_index_file(const std::filesystem::path& path,
                      const std::vector<SegmentLog::IndexEntry>& entries) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return;
  for (const auto& entry : entries) {
    out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    out.write(reinterpret_cast<const char*>(&entry.position), sizeof(entry.position));
  }
}

std::vector<SegmentLog::IndexEntry> read_index_file(const std::filesystem::path& path) {
  std::vector<SegmentLog::IndexEntry> entries;
  std::ifstream in(path, std::ios::binary);
  if (!in) return entries;
  SegmentLog::IndexEntry entry{};
  while (in.read(reinterpret_cast<char*>(&entry.offset), sizeof(entry.offset)) &&
         in.read(reinterpret_cast<char*>(&entry.position), sizeof(entry.position))) {
    entries.push_back(entry);
  }
  return entries;
}

}  // namespace

SegmentLog::SegmentLog(std::filesystem::path directory, std::uint64_t base_offset,
                       std::size_t maximum_record_body_size)
    : SegmentLog(std::move(directory), base_offset,
                 SegmentLogOptions{.maximum_record_body_size = maximum_record_body_size}) {}

SegmentLog::SegmentLog(std::filesystem::path directory, std::uint64_t base_offset,
                       SegmentLogOptions options)
    : directory_(std::move(directory)), base_offset_(base_offset), next_offset_(base_offset),
      options_(options) {
  if (options_.maximum_record_body_size == 0 || options_.maximum_segment_bytes == 0) {
    throw std::invalid_argument("segment limits must be positive");
  }
}

void SegmentLog::build_or_load_index(Segment& segment) {
  segment.index = read_index_file(segment.index_path);

  // Validate index entries against actual segment bytes
  bool index_valid = !segment.index.empty();
  if (index_valid) {
    if (segment.index.front().offset != segment.base_offset ||
        segment.index.back().position >= segment.bytes) {
      index_valid = false;
    }
  }

  if (index_valid) return;

  // Build index by scanning the segment
  segment.index.clear();
  const auto bytes = read_bytes(segment.path);
  const auto scan = scan_records(bytes, segment.base_offset, options_.maximum_record_body_size);
  if (scan.disposition == ScanDisposition::kComplete) {
    std::uint64_t curr_pos = 0;
    for (const auto& rec : scan.records) {
      segment.index.push_back(IndexEntry{rec.offset, curr_pos});
      curr_pos += encode_record(rec, options_.maximum_record_body_size).size();
    }
    write_index_file(segment.index_path, segment.index);
  }
}

void SegmentLog::recover() {
  std::filesystem::create_directories(directory_);
  segments_.clear();
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (!entry.is_regular_file()) continue;
    const auto base = parse_segment_base(entry.path());
    if (!base) {
      if (entry.path().extension() == ".log") throw std::runtime_error("invalid segment filename: " + entry.path().string());
      continue;
    }
    segments_.push_back(Segment{*base, *base, entry.path(), index_path_for(entry.path()), 0, {}});
  }
  if (segments_.empty()) {
    const auto path = segment_path(directory_, base_offset_);
    std::ofstream create(path, std::ios::binary);
    if (!create) throw std::runtime_error("cannot create segment: " + path.string());
    segments_.push_back(Segment{base_offset_, base_offset_, path, index_path_for(path), 0, {}});
  }
  std::sort(segments_.begin(), segments_.end(), [](const Segment& left, const Segment& right) { return left.base_offset < right.base_offset; });
  // Derive the effective base offset from what is actually on disk. The
  // constructor-supplied base_offset_ is only used when no segments exist yet
  // (the empty-directory branch above). After retention prunes the oldest
  // segment(s), the first surviving segment will have a non-zero base offset
  // that must be accepted here rather than rejected.
  base_offset_ = segments_.front().base_offset;
  std::uint64_t expected = base_offset_;
  for (std::size_t index = 0; index < segments_.size(); ++index) {
    auto& segment = segments_[index];
    if (segment.base_offset != expected) throw std::runtime_error("segment offsets are not contiguous");
    const auto bytes = read_bytes(segment.path);
    const auto scan = scan_records(bytes, expected, options_.maximum_record_body_size);
    if (scan.disposition == ScanDisposition::kCorruption) throw std::runtime_error("segment corruption at byte " + std::to_string(scan.valid_bytes) + ": " + scan.error.message);
    if (scan.disposition == ScanDisposition::kTruncatedTail) {
      if (index + 1U != segments_.size()) throw std::runtime_error("non-active segment has a truncated tail");
      std::filesystem::resize_file(segment.path, scan.valid_bytes);
      // Remove stale index to ensure no torn records remain referenced
      std::error_code ec;
      std::filesystem::remove(segment.index_path, ec);
    }
    segment.bytes = scan.valid_bytes;
    segment.next_offset = scan.next_offset;
    expected = scan.next_offset;

    // Load or rebuild index
    build_or_load_index(segment);
  }
  next_offset_ = expected;
}

AppendResult SegmentLog::append(std::vector<Record> records, AcknowledgementMode mode, AppendTiming* timing) {
  const auto append_started = std::chrono::steady_clock::now();
  if (records.empty()) {
    throw std::invalid_argument("cannot append an empty record batch");
  }
  const auto first_offset = next_offset_;
  std::vector<std::vector<std::byte>> encoded_records;
  encoded_records.reserve(records.size());
  std::uint64_t candidate_offset = first_offset;
  for (auto& record : records) {
    record.offset = candidate_offset++;
    encoded_records.push_back(encode_record(record, options_.maximum_record_body_size));
  }
  std::size_t encoded_batch_bytes = 0;
  for (const auto& bytes : encoded_records) encoded_batch_bytes += bytes.size();
  roll_if_needed(encoded_batch_bytes);

  auto& active = segments_.back();
  std::ofstream output(active.path, std::ios::binary | std::ios::app);
  if (!output) {
    throw std::runtime_error("cannot open segment for append: " + active.path.string());
  }

  std::ofstream index_output(active.index_path, std::ios::binary | std::ios::app);
  std::uint64_t curr_pos = active.bytes;

  for (std::size_t i = 0; i < encoded_records.size(); ++i) {
    const auto& bytes = encoded_records[i];
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    if (!output) {
      throw std::runtime_error("failed appending record to segment: " + active.path.string());
    }

    const std::uint64_t rec_offset = first_offset + i;
    active.index.push_back(IndexEntry{rec_offset, curr_pos});
    if (index_output) {
      index_output.write(reinterpret_cast<const char*>(&rec_offset), sizeof(rec_offset));
      index_output.write(reinterpret_cast<const char*>(&curr_pos), sizeof(curr_pos));
    }
    curr_pos += bytes.size();
  }

  const auto flush_started = std::chrono::steady_clock::now();
  output.flush();
  if (index_output) index_output.flush();
  if (!output) {
    throw std::runtime_error("failed flushing segment stream: " + active.path.string());
  }
  if (mode == AcknowledgementMode::kSynced) {
    synchronize();
  }
  next_offset_ = candidate_offset;
  active.next_offset = candidate_offset;
  active.bytes += encoded_batch_bytes;
  if (timing != nullptr) {
    timing->append_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - append_started).count());
    timing->flush_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - flush_started).count());
  }
  return AppendResult{first_offset, candidate_offset - 1};
}

std::vector<Record> SegmentLog::fetch(std::uint64_t start_offset,
                                      std::size_t maximum_records) const {
  if (start_offset < base_offset_ || start_offset > next_offset_) {
    throw std::out_of_range("requested offset is outside the retained range");
  }
  std::vector<Record> fetched;
  for (const auto& segment : segments_) {
    if (segment.next_offset <= start_offset || fetched.size() == maximum_records) continue;
    auto records = scan_or_throw(segment, start_offset, maximum_records - fetched.size());
    fetched.insert(fetched.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
  }
  return fetched;
}

RetentionStats SegmentLog::enforce_retention() {
  RetentionStats stats{};
  if (options_.retention_bytes == 0 || segments_.size() <= 1) {
    return stats;
  }

  std::size_t current_total = total_bytes();
  while (segments_.size() > 1 && current_total > options_.retention_bytes) {
    auto oldest = segments_.front();
    std::error_code ec;
    std::filesystem::remove(oldest.path, ec);
    std::filesystem::remove(oldest.index_path, ec);

    current_total -= oldest.bytes;
    stats.bytes_reclaimed += oldest.bytes;
    stats.segments_deleted++;

    segments_.erase(segments_.begin());
    base_offset_ = segments_.front().base_offset;
  }

  return stats;
}

std::uint64_t SegmentLog::next_offset() const noexcept { return next_offset_; }
std::uint64_t SegmentLog::base_offset() const noexcept { return base_offset_; }
const std::filesystem::path& SegmentLog::path() const noexcept { return segments_.back().path; }
std::size_t SegmentLog::segment_count() const noexcept { return segments_.size(); }

std::size_t SegmentLog::total_bytes() const noexcept {
  std::size_t sum = 0;
  for (const auto& segment : segments_) sum += segment.bytes;
  return sum;
}

void SegmentLog::synchronize() const {
#ifdef __unix__
  const int descriptor = ::open(segments_.back().path.c_str(), O_RDONLY);
  if (descriptor < 0) {
    throw std::runtime_error("cannot open segment for synchronization: " + segments_.back().path.string());
  }
  const int result = ::fsync(descriptor);
  const int close_result = ::close(descriptor);
  if (result != 0 || close_result != 0) {
    throw std::runtime_error("segment synchronization failed: " + segments_.back().path.string());
  }
#else
  throw std::runtime_error("SYNCED acknowledgement is supported only on POSIX targets");
#endif
}

std::vector<Record> SegmentLog::scan_or_throw(const Segment& segment, std::uint64_t start_offset,
                                              std::size_t maximum_records) const {
  std::uint64_t seek_position = 0;
  std::uint64_t scan_base_offset = segment.base_offset;

  if (!segment.index.empty() && start_offset > segment.base_offset) {
    auto it = std::lower_bound(segment.index.begin(), segment.index.end(), start_offset,
                               [](const IndexEntry& entry, std::uint64_t val) {
                                 return entry.offset < val;
                               });
    if (it != segment.index.begin() && (it == segment.index.end() || it->offset > start_offset)) {
      --it;
    }
    if (it != segment.index.end() && it->offset <= start_offset) {
      seek_position = it->position;
      scan_base_offset = it->offset;
    }
  }

  const auto bytes = read_bytes(segment.path);
  if (seek_position > bytes.size()) {
    throw std::runtime_error("invalid index seek position in segment");
  }

  std::span<const std::byte> slice(bytes.data() + seek_position, bytes.size() - seek_position);
  const auto scan = scan_records(slice, scan_base_offset, options_.maximum_record_body_size);
  if (scan.disposition != ScanDisposition::kComplete) {
    throw std::runtime_error("segment is not a complete valid log");
  }
  std::vector<Record> records;
  for (auto& record : scan.records) {
    if (record.offset >= start_offset && records.size() < maximum_records) records.push_back(std::move(record));
  }
  return records;
}

void SegmentLog::roll_if_needed(std::size_t encoded_batch_bytes) {
  auto& active = segments_.back();
  if (active.bytes == 0 || active.bytes + encoded_batch_bytes <= options_.maximum_segment_bytes) return;
  const auto path = segment_path(directory_, next_offset_);
  std::ofstream create(path, std::ios::binary);
  if (!create) throw std::runtime_error("cannot create segment: " + path.string());
  segments_.push_back(Segment{next_offset_, next_offset_, path, index_path_for(path), 0, {}});
}

}  // namespace streamforge::storage

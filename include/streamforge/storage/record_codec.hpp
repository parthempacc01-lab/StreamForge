#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace streamforge::storage {

inline constexpr std::uint8_t kRecordVersion = 1;
inline constexpr std::size_t kRecordLengthSize = 4;
inline constexpr std::size_t kRecordHeaderSize = 26;
inline constexpr std::size_t kRecordChecksumSize = 4;
inline constexpr std::size_t kMinimumRecordBodySize =
    kRecordHeaderSize + kRecordChecksumSize;
inline constexpr std::size_t kDefaultMaximumRecordBodySize = 1U * 1024U * 1024U;

struct Record {
  std::uint64_t offset{0};
  std::int64_t timestamp_unix_ms{0};
  std::vector<std::byte> key;
  std::vector<std::byte> value;
};

enum class RecordErrorCode {
  kOversizedRecord,
  kUnsupportedVersion,
  kInvalidLengths,
  kChecksumMismatch,
  kUnexpectedOffset,
};

struct RecordError {
  RecordErrorCode code;
  std::string message;
};

enum class ScanDisposition {
  kComplete,
  kTruncatedTail,
  kCorruption,
};

struct ScanResult {
  ScanDisposition disposition{ScanDisposition::kComplete};
  std::vector<Record> records;
  std::size_t valid_bytes{0};
  std::uint64_t next_offset{0};
  RecordError error{RecordErrorCode::kInvalidLengths, ""};
};

// CRC-32C (Castagnoli) over the supplied bytes. It is used to distinguish an
// intact record from corruption after a complete record has reached a segment.
[[nodiscard]] std::uint32_t crc32c(std::span<const std::byte> bytes) noexcept;

// Encodes one self-contained record. The initial four-byte body length is not
// included in its value and is always big-endian on disk.
[[nodiscard]] std::vector<std::byte> encode_record(
    const Record& record,
    std::size_t maximum_record_body_size = kDefaultMaximumRecordBodySize);

// Scans one segment image. Only an incomplete final length prefix or body is
// recoverable as a truncated tail. Invalid completed records are corruption.
[[nodiscard]] ScanResult scan_records(
    std::span<const std::byte> bytes,
    std::uint64_t expected_first_offset,
    std::size_t maximum_record_body_size = kDefaultMaximumRecordBodySize);

}  // namespace streamforge::storage

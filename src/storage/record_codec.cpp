#include "streamforge/storage/record_codec.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace streamforge::storage {
namespace {

void append_u32(std::vector<std::byte>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
  }
}

void append_u64(std::vector<std::byte>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
  }
}

std::uint32_t read_u32(std::span<const std::byte> input, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = (value << 8U) | static_cast<std::uint32_t>(input[offset + index]);
  }
  return value;
}

std::uint64_t read_u64(std::span<const std::byte> input, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<std::uint64_t>(input[offset + index]);
  }
  return value;
}

void append_i64(std::vector<std::byte>& output, std::int64_t value) {
  append_u64(output, static_cast<std::uint64_t>(value));
}

std::int64_t read_i64(std::span<const std::byte> input, std::size_t offset) {
  return static_cast<std::int64_t>(read_u64(input, offset));
}

ScanResult corruption(std::size_t valid_bytes, std::uint64_t next_offset,
                      RecordErrorCode code, std::string message) {
  return ScanResult{.disposition = ScanDisposition::kCorruption,
                    .records = {},
                    .valid_bytes = valid_bytes,
                    .next_offset = next_offset,
                    .error = RecordError{code, std::move(message)}};
}

}  // namespace

std::uint32_t crc32c(std::span<const std::byte> bytes) noexcept {
  std::uint32_t crc = 0xFFFFFFFFU;
  for (const auto byte : bytes) {
    crc ^= static_cast<std::uint8_t>(byte);
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82F63B78U & mask);
    }
  }
  return ~crc;
}

std::vector<std::byte> encode_record(const Record& record,
                                     std::size_t maximum_record_body_size) {
  const auto payload_size = record.key.size() + record.value.size();
  if (record.key.size() > std::numeric_limits<std::uint32_t>::max() ||
      record.value.size() > std::numeric_limits<std::uint32_t>::max() ||
      payload_size < record.key.size()) {
    throw std::length_error("record key or value exceeds the format limit");
  }
  const auto body_size = kMinimumRecordBodySize + payload_size;
  if (body_size > maximum_record_body_size ||
      body_size > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error("record body exceeds the configured limit");
  }

  std::vector<std::byte> encoded;
  encoded.reserve(kRecordLengthSize + body_size);
  append_u32(encoded, static_cast<std::uint32_t>(body_size));
  encoded.push_back(static_cast<std::byte>(kRecordVersion));
  encoded.push_back(std::byte{0});  // Attributes reserved for a future version.
  append_u64(encoded, record.offset);
  append_i64(encoded, record.timestamp_unix_ms);
  append_u32(encoded, static_cast<std::uint32_t>(record.key.size()));
  append_u32(encoded, static_cast<std::uint32_t>(record.value.size()));
  encoded.insert(encoded.end(), record.key.begin(), record.key.end());
  encoded.insert(encoded.end(), record.value.begin(), record.value.end());

  const std::span<const std::byte> checksummed{encoded.data() + kRecordLengthSize,
                                                body_size - kRecordChecksumSize};
  append_u32(encoded, crc32c(checksummed));
  return encoded;
}

ScanResult scan_records(std::span<const std::byte> bytes,
                        std::uint64_t expected_first_offset,
                        std::size_t maximum_record_body_size) {
  ScanResult result;
  result.next_offset = expected_first_offset;
  std::size_t cursor = 0;

  while (cursor < bytes.size()) {
    const auto remaining = bytes.size() - cursor;
    if (remaining < kRecordLengthSize) {
      result.disposition = ScanDisposition::kTruncatedTail;
      result.valid_bytes = cursor;
      return result;
    }

    const auto body_size = static_cast<std::size_t>(read_u32(bytes, cursor));
    if (body_size < kMinimumRecordBodySize || body_size > maximum_record_body_size) {
      return corruption(cursor, result.next_offset, RecordErrorCode::kOversizedRecord,
                        "record body length is invalid");
    }
    if (body_size > remaining - kRecordLengthSize) {
      result.disposition = ScanDisposition::kTruncatedTail;
      result.valid_bytes = cursor;
      return result;
    }

    const std::span<const std::byte> body{bytes.data() + cursor + kRecordLengthSize,
                                          body_size};
    if (static_cast<std::uint8_t>(body[0]) != kRecordVersion) {
      return corruption(cursor, result.next_offset, RecordErrorCode::kUnsupportedVersion,
                        "unsupported record version");
    }

    const auto key_size = static_cast<std::size_t>(read_u32(body, 18));
    const auto value_size = static_cast<std::size_t>(read_u32(body, 22));
    const auto payload_size = body_size - kMinimumRecordBodySize;
    if (key_size > payload_size || value_size != payload_size - key_size) {
      return corruption(cursor, result.next_offset, RecordErrorCode::kInvalidLengths,
                        "key and value lengths do not match the record body");
    }

    const auto stored_crc = read_u32(body, body_size - kRecordChecksumSize);
    const std::span<const std::byte> checksummed{body.data(),
                                                 body_size - kRecordChecksumSize};
    if (crc32c(checksummed) != stored_crc) {
      return corruption(cursor, result.next_offset, RecordErrorCode::kChecksumMismatch,
                        "record checksum does not match");
    }

    const auto offset = read_u64(body, 2);
    if (offset != result.next_offset) {
      return corruption(cursor, result.next_offset, RecordErrorCode::kUnexpectedOffset,
                        "record offset is not contiguous");
    }

    Record record;
    record.offset = offset;
    record.timestamp_unix_ms = read_i64(body, 10);
    const auto payload = body.subspan(kRecordHeaderSize, payload_size);
    record.key.assign(payload.begin(), payload.begin() + static_cast<std::ptrdiff_t>(key_size));
    record.value.assign(payload.begin() + static_cast<std::ptrdiff_t>(key_size), payload.end());
    result.records.push_back(std::move(record));
    ++result.next_offset;
    cursor += kRecordLengthSize + body_size;
  }

  result.valid_bytes = cursor;
  return result;
}

}  // namespace streamforge::storage

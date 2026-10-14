#include "streamforge/storage/record_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace streamforge::storage {
namespace {

Record make_record(std::uint64_t offset) {
  return Record{.offset = offset,
                .timestamp_unix_ms = 1'725'000'000'000,
                .key = {std::byte{0x01}, std::byte{0x02}},
                .value = {std::byte{0xA0}, std::byte{0xB0}, std::byte{0xC0}}};
}

TEST(RecordCodec, EncodesAndScansAValidRecord) {
  const auto bytes = encode_record(make_record(7));
  const auto result = scan_records(bytes, 7);

  ASSERT_EQ(result.disposition, ScanDisposition::kComplete);
  ASSERT_EQ(result.records.size(), 1U);
  EXPECT_EQ(result.records[0].offset, 7U);
  EXPECT_EQ(result.records[0].timestamp_unix_ms, 1'725'000'000'000);
  EXPECT_EQ(result.records[0].key, make_record(7).key);
  EXPECT_EQ(result.records[0].value, make_record(7).value);
  EXPECT_EQ(result.next_offset, 8U);
  EXPECT_EQ(result.valid_bytes, bytes.size());
}

TEST(RecordCodec, ScansContiguousRecords) {
  auto bytes = encode_record(make_record(11));
  const auto second = encode_record(make_record(12));
  bytes.insert(bytes.end(), second.begin(), second.end());

  const auto result = scan_records(bytes, 11);

  ASSERT_EQ(result.disposition, ScanDisposition::kComplete);
  ASSERT_EQ(result.records.size(), 2U);
  EXPECT_EQ(result.next_offset, 13U);
}

TEST(RecordCodec, ClassifiesAnIncompleteFinalRecordAsRecoverableTail) {
  auto bytes = encode_record(make_record(0));
  const auto second = encode_record(make_record(1));
  bytes.insert(bytes.end(), second.begin(), second.end() - 2);

  const auto result = scan_records(bytes, 0);

  EXPECT_EQ(result.disposition, ScanDisposition::kTruncatedTail);
  EXPECT_EQ(result.records.size(), 1U);
  EXPECT_EQ(result.valid_bytes, encode_record(make_record(0)).size());
  EXPECT_EQ(result.next_offset, 1U);
}

TEST(RecordCodec, RejectsChecksumFailureAsCorruptionEvenAtTail) {
  auto bytes = encode_record(make_record(0));
  bytes[15] ^= std::byte{0x01};

  const auto result = scan_records(bytes, 0);

  EXPECT_EQ(result.disposition, ScanDisposition::kCorruption);
  EXPECT_EQ(result.error.code, RecordErrorCode::kChecksumMismatch);
}

TEST(RecordCodec, RejectsACompletedRecordWithAnUnexpectedOffset) {
  const auto bytes = encode_record(make_record(2));
  const auto result = scan_records(bytes, 0);

  EXPECT_EQ(result.disposition, ScanDisposition::kCorruption);
  EXPECT_EQ(result.error.code, RecordErrorCode::kUnexpectedOffset);
}

}  // namespace
}  // namespace streamforge::storage

#include "streamforge/storage/segment_log.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

namespace streamforge::storage {
namespace {

class SegmentLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = std::filesystem::temp_directory_path() /
                 ("streamforge-segment-test-" +
                  std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  void TearDown() override { std::filesystem::remove_all(directory_); }
  std::filesystem::path directory_;
};

TEST_F(SegmentLogTest, AppendsFetchesAndRecoversOffsets) {
  SegmentLog log(directory_, 0);
  log.recover();
  auto result = log.append({Record{.value = {std::byte{0x01}}},
                            Record{.value = {std::byte{0x02}}}},
                           AcknowledgementMode::kAppended);
  EXPECT_EQ(result.first_offset, 0U);
  EXPECT_EQ(result.last_offset, 1U);
  EXPECT_EQ(log.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x02}}));

  SegmentLog restarted(directory_, 0);
  restarted.recover();
  EXPECT_EQ(restarted.next_offset(), 2U);
}

TEST_F(SegmentLogTest, RepairsOnlyATornFinalRecordAndRecoveryIsIdempotent) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append({Record{.value = {std::byte{0x10}}}, Record{.value = {std::byte{0x20}}}},
             AcknowledgementMode::kAppended);
  const auto intact_size = std::filesystem::file_size(log.path());
  const auto torn = encode_record(Record{.offset = 2, .value = {std::byte{0x30}}});
  { std::ofstream output(log.path(), std::ios::binary | std::ios::app);
    output.write(reinterpret_cast<const char*>(torn.data()),
                 static_cast<std::streamsize>(torn.size() - 3)); }

  SegmentLog restarted(directory_, 0);
  restarted.recover();
  EXPECT_EQ(restarted.next_offset(), 2U);
  EXPECT_EQ(restarted.fetch(0, 10).size(), 2U);
  EXPECT_EQ(std::filesystem::file_size(restarted.path()), intact_size);

  SegmentLog repeated(directory_, 0);
  repeated.recover();
  EXPECT_EQ(repeated.next_offset(), 2U);
  EXPECT_EQ(repeated.fetch(0, 10).size(), 2U);
}

TEST_F(SegmentLogTest, RepairsAnIncompleteFinalLengthPrefixAfterValidRecords) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append({Record{.value = {std::byte{0x10}}}}, AcknowledgementMode::kAppended);
  const auto intact_size = std::filesystem::file_size(log.path());
  { std::ofstream output(log.path(), std::ios::binary | std::ios::app);
    const char bytes[] = {0x00, 0x00}; output.write(bytes, 2); }

  SegmentLog restarted(directory_, 0);
  restarted.recover();
  EXPECT_EQ(restarted.next_offset(), 1U);
  EXPECT_EQ(restarted.fetch(0, 10).size(), 1U);
  EXPECT_EQ(std::filesystem::file_size(restarted.path()), intact_size);
}

TEST_F(SegmentLogTest, RejectsChecksumCorruptionAfterValidRecords) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append({Record{.value = {std::byte{0x10}}}, Record{.value = {std::byte{0x20}}}},
             AcknowledgementMode::kAppended);
  const auto size = std::filesystem::file_size(log.path());
  { std::fstream file(log.path(), std::ios::binary | std::ios::in | std::ios::out);
    file.seekg(static_cast<std::streamoff>(size - 1)); char corrupt{}; file.read(&corrupt, 1);
    corrupt ^= static_cast<char>(0xFF); file.seekp(static_cast<std::streamoff>(size - 1));
    file.write(&corrupt, 1); }

  SegmentLog restarted(directory_, 0);
  EXPECT_THROW(restarted.recover(), std::runtime_error);
  EXPECT_EQ(std::filesystem::file_size(log.path()), size);
}

TEST_F(SegmentLogTest, SyncedAppendIsReadableAfterRestart) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append({Record{.value = {std::byte{0x55}}}}, AcknowledgementMode::kSynced);

  SegmentLog restarted(directory_, 0);
  restarted.recover();
  EXPECT_EQ(restarted.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x55}}));
}

TEST_F(SegmentLogTest, CreatesAndPersistsIndexFilesAcrossRollAndRestart) {
  // Use small maximum_segment_bytes to trigger segment rolling
  SegmentLogOptions options;
  options.maximum_segment_bytes = 64; // forces roll after ~2 small records
  SegmentLog log(directory_, 0, options);
  log.recover();

  for (int i = 0; i < 6; ++i) {
    log.append({Record{.value = {std::byte(static_cast<unsigned char>(i))}}}, AcknowledgementMode::kAppended);
  }

  EXPECT_GT(log.segment_count(), 1U);

  // Verify that .index files exist on disk for segments
  std::size_t index_files_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_files_count++;
      EXPECT_GT(std::filesystem::file_size(entry.path()), 0U);
    }
  }
  EXPECT_EQ(index_files_count, log.segment_count());

  // Recover in a new log instance and verify offsets and continuity
  SegmentLog restarted(directory_, 0, options);
  restarted.recover();
  EXPECT_EQ(restarted.segment_count(), log.segment_count());
  EXPECT_EQ(restarted.next_offset(), 6U);
  EXPECT_EQ(restarted.fetch(3, 1).front().value, (std::vector<std::byte>{std::byte{0x03}}));
}

TEST_F(SegmentLogTest, EnforcesRetentionPolicyByDeletingOldestSegments) {
  SegmentLogOptions options;
  options.maximum_segment_bytes = 64;
  SegmentLog log(directory_, 0, options);
  log.recover();

  for (int i = 0; i < 8; ++i) {
    log.append({Record{.value = {std::byte(static_cast<unsigned char>(i))}}}, AcknowledgementMode::kAppended);
  }
  const auto initial_segments = log.segment_count();
  EXPECT_GT(initial_segments, 2U);

  // Set retention limit to keep only ~last 2 segments
  options.retention_bytes = 150;
  SegmentLog retained_log(directory_, 0, options);
  retained_log.recover();
  const auto stats = retained_log.enforce_retention();

  EXPECT_GT(stats.segments_deleted, 0U);
  EXPECT_GT(stats.bytes_reclaimed, 0U);
  EXPECT_LT(retained_log.segment_count(), initial_segments);
  EXPECT_GT(retained_log.base_offset(), 0U);

  // Cannot fetch deleted offset; fetch within retained range succeeds
  EXPECT_THROW(retained_log.fetch(0, 1), std::out_of_range);
  EXPECT_NO_THROW(retained_log.fetch(retained_log.base_offset(), 1));
}

TEST_F(SegmentLogTest, RebuildsIndexAutomaticallyIfDeleted) {
  SegmentLog log(directory_, 0);
  log.recover();
  const auto res = log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);
  (void)res;

  // Remove the companion index file
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      std::filesystem::remove(entry.path());
    }
  }

  // Next recovery must detect missing index and rebuild it cleanly
  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));

  // Confirm .index file was recreated and contains valid entries
  bool index_recreated = false;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_recreated = true;
      EXPECT_EQ(std::filesystem::file_size(entry.path()), 3U * sizeof(SegmentLog::IndexEntry));
    }
  }
  EXPECT_TRUE(index_recreated);
}

TEST_F(SegmentLogTest, RebuildsIndexWhenIntermediatePositionAltered) {
  SegmentLog log(directory_, 0);
  log.recover();
  const auto res = log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}},
       Record{.value = {std::byte{0x40}}}},
      AcknowledgementMode::kAppended);
  (void)res;

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());
  ASSERT_EQ(std::filesystem::file_size(index_path), 4U * sizeof(SegmentLog::IndexEntry));

  // Alter intermediate index entry 1's position to point to an invalid byte within the record payload
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.is_open());
    // Entry 1 starts at byte 16 (entry 0 is bytes 0..15).
    // Entry 1 position is stored at bytes 24..31 (offset at 16..23).
    file.seekg(24);
    std::uint64_t pos = 0;
    file.read(reinterpret_cast<char*>(&pos), sizeof(pos));
    pos += 5;  // Shift position into the middle of the record payload
    file.seekp(24);
    file.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
    file.flush();
  }

  // Recovery must detect invalid position / boundary mismatch, rebuild from log, and restore correctness
  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 4U);

  // Verify that every valid offset returns the correct record after recovery
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
  EXPECT_EQ(recovered.fetch(3, 1).front().value, (std::vector<std::byte>{std::byte{0x40}}));

  // Verify index file was rewritten with valid entries
  EXPECT_EQ(std::filesystem::file_size(index_path), 4U * sizeof(SegmentLog::IndexEntry));
}

TEST_F(SegmentLogTest, RebuildsIndexWhenOffsetCorrupted) {
  SegmentLog log(directory_, 0);
  log.recover();
  const auto res = log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}},
       Record{.value = {std::byte{0x40}}}},
      AcknowledgementMode::kAppended);
  (void)res;

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Corrupt intermediate index entry 2's offset (bytes 32..39) to a non-contiguous offset
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.is_open());
    file.seekp(32);
    const std::uint64_t bogus_offset = 999;
    file.write(reinterpret_cast<const char*>(&bogus_offset), sizeof(bogus_offset));
    file.flush();
  }

  // Recovery must detect corrupted offset, rebuild the index, and restore full functionality
  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 4U);

  // Verify that every valid offset returns the correct record after recovery
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
  EXPECT_EQ(recovered.fetch(3, 1).front().value, (std::vector<std::byte>{std::byte{0x40}}));

  // Verify index file on disk has been corrected
  {
    std::ifstream file(index_path, std::ios::binary);
    file.seekg(32);
    std::uint64_t offset = 0;
    file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
    EXPECT_EQ(offset, 2U);
  }
}

TEST_F(SegmentLogTest, RebuildsIndexWhenFileTruncated) {
  SegmentLog log(directory_, 0);
  log.recover();
  const auto res = log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}},
       Record{.value = {std::byte{0x40}}}},
      AcknowledgementMode::kAppended);
  (void)res;

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());
  ASSERT_EQ(std::filesystem::file_size(index_path), 64U);

  // Case 1: Truncate index file to 32 bytes (only 2 complete entries out of 4)
  std::filesystem::resize_file(index_path, 32);

  SegmentLog recovered1(directory_, 0);
  recovered1.recover();
  EXPECT_EQ(recovered1.next_offset(), 4U);
  EXPECT_EQ(recovered1.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered1.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered1.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
  EXPECT_EQ(recovered1.fetch(3, 1).front().value, (std::vector<std::byte>{std::byte{0x40}}));
  EXPECT_EQ(std::filesystem::file_size(index_path), 64U);

  // Case 2: Truncate index file to 24 bytes (1 complete entry + 8 trailing bytes of torn entry)
  std::filesystem::resize_file(index_path, 24);

  SegmentLog recovered2(directory_, 0);
  recovered2.recover();
  EXPECT_EQ(recovered2.next_offset(), 4U);
  EXPECT_EQ(recovered2.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered2.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered2.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
  EXPECT_EQ(recovered2.fetch(3, 1).front().value, (std::vector<std::byte>{std::byte{0x40}}));
  EXPECT_EQ(std::filesystem::file_size(index_path), 64U);
}

TEST_F(SegmentLogTest, CorruptedIndexEntryIsDetected) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Corrupt intermediate index entry with random bytes
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekp(16);
    const char garbage[16] = {char(0xDE), char(0xAD), char(0xBE), char(0xEF)};
    file.write(garbage, sizeof(garbage));
    file.flush();
  }

  // Corrupted index entry detected -> rebuilt from authoritative log
  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
}

TEST_F(SegmentLogTest, TruncatedIndexFileIsDetected) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Truncate to unaligned byte boundary (20 bytes instead of 48)
  std::filesystem::resize_file(index_path, 20);

  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
  EXPECT_EQ(std::filesystem::file_size(index_path), 3U * sizeof(SegmentLog::IndexEntry));
}

TEST_F(SegmentLogTest, IndexWithWrongPositionIsDetected) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Set position to point past the end of the segment log file
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekp(sizeof(std::uint64_t)); // position of entry 0
    const std::uint64_t invalid_pos = 999999;
    file.write(reinterpret_cast<const char*>(&invalid_pos), sizeof(invalid_pos));
    file.flush();
  }

  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
}

TEST_F(SegmentLogTest, IndexWithWrongOffsetIsDetected) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Set entry 0 offset to 42 instead of base_offset 0
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekp(0);
    const std::uint64_t wrong_offset = 42;
    file.write(reinterpret_cast<const char*>(&wrong_offset), sizeof(wrong_offset));
    file.flush();
  }

  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
}

TEST_F(SegmentLogTest, IndexEntriesMustBeMonotonic) {
  SegmentLog log(directory_, 0);
  log.recover();
  log.append(
      {Record{.value = {std::byte{0x10}}},
       Record{.value = {std::byte{0x20}}},
       Record{.value = {std::byte{0x30}}}},
      AcknowledgementMode::kAppended);

  std::filesystem::path index_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".index") {
      index_path = entry.path();
      break;
    }
  }
  ASSERT_FALSE(index_path.empty());

  // Make entry 2 position non-monotonic (0 <= entry 1 position)
  {
    std::fstream file(index_path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekp(40);
    const std::uint64_t non_monotonic_pos = 0;
    file.write(reinterpret_cast<const char*>(&non_monotonic_pos), sizeof(non_monotonic_pos));
    file.flush();
  }

  SegmentLog recovered(directory_, 0);
  recovered.recover();
  EXPECT_EQ(recovered.next_offset(), 3U);
  EXPECT_EQ(recovered.fetch(0, 1).front().value, (std::vector<std::byte>{std::byte{0x10}}));
  EXPECT_EQ(recovered.fetch(1, 1).front().value, (std::vector<std::byte>{std::byte{0x20}}));
  EXPECT_EQ(recovered.fetch(2, 1).front().value, (std::vector<std::byte>{std::byte{0x30}}));
}

TEST_F(SegmentLogTest, BoundedSeekReadFetchesOnlyRequestedSlice) {
  SegmentLog log(directory_, 0);
  log.recover();
  for (std::uint8_t i = 0; i < 10; ++i) {
    log.append({Record{.value = {std::byte{i}}}}, AcknowledgementMode::kAppended);
  }

  // Fetch middle slice [4..6] (3 records)
  const auto slice = log.fetch(4, 3);
  ASSERT_EQ(slice.size(), 3U);
  EXPECT_EQ(slice[0].offset, 4U);
  EXPECT_EQ(slice[0].value.front(), std::byte{4});
  EXPECT_EQ(slice[1].offset, 5U);
  EXPECT_EQ(slice[1].value.front(), std::byte{5});
  EXPECT_EQ(slice[2].offset, 6U);
  EXPECT_EQ(slice[2].value.front(), std::byte{6});
}

// Regression: after retention prunes the oldest segments and advances
// base_offset_, recover() in a new SegmentLog instance (simulating a process
// restart) must accept the non-zero first-segment base offset rather than
// throwing "first segment has an unexpected base offset".
TEST_F(SegmentLogTest, RetentionSurvivesRestart) {
  SegmentLogOptions options;
  options.maximum_segment_bytes = 64;
  options.retention_bytes = 150;

  // Phase 1: write enough data to create multiple segments, then prune.
  {
    SegmentLog log(directory_, 0, options);
    log.recover();
    for (int i = 0; i < 8; ++i) {
      log.append({Record{.value = {std::byte(static_cast<unsigned char>(i))}}},
                 AcknowledgementMode::kAppended);
    }
    const auto stats = log.enforce_retention();
    ASSERT_GT(stats.segments_deleted, 0U) << "precondition: retention must have deleted segments";
    ASSERT_GT(log.base_offset(), 0U) << "precondition: base_offset must have advanced";
  }

  // Phase 2: recover in a brand-new instance — this is the restart.
  SegmentLog restarted(directory_, 0, options);
  ASSERT_NO_THROW(restarted.recover())
      << "recover() must not throw after retention pruned the leading segments";

  // Offsets before the new base are gone.
  EXPECT_THROW(restarted.fetch(0, 1), std::out_of_range);
  // Offsets within the retained window are readable.
  EXPECT_NO_THROW(restarted.fetch(restarted.base_offset(), 1));

  // Appending after restart and recovering a third time must also succeed.
  restarted.append({Record{.value = {std::byte{0xAB}}}},
                   AcknowledgementMode::kAppended);
  const std::uint64_t after_next = restarted.next_offset();

  SegmentLog third(directory_, 0, options);
  ASSERT_NO_THROW(third.recover());
  EXPECT_EQ(third.next_offset(), after_next);
}

// Regression: enforce_retention() is idempotent and safe to call after appends.
// The end-to-end integration test through BrokerService::produce() and broker restart
// is tested in BrokerServiceTest.ConfiguredRetentionEnforcesAndSurvivesRestart.
TEST_F(SegmentLogTest, RetentionEnforcedAutomaticallyDoesNotAccumulateSegments) {
  SegmentLogOptions options;
  options.maximum_segment_bytes = 64;
  options.retention_bytes = 150;

  SegmentLog log(directory_, 0, options);
  log.recover();

  // Write well beyond the retention limit and call enforce_retention() after
  // each append (mirroring what BrokerService::produce() now does).
  for (int i = 0; i < 20; ++i) {
    log.append({Record{.value = {std::byte(static_cast<unsigned char>(i))}}},
               AcknowledgementMode::kAppended);
    log.enforce_retention();
  }

  // Total bytes must be at or below the configured retention limit.
  EXPECT_LE(log.total_bytes(), options.retention_bytes);
  // At least one segment must remain (never delete the active segment).
  EXPECT_GE(log.segment_count(), 1U);
  // The base offset must have advanced beyond zero.
  EXPECT_GT(log.base_offset(), 0U);
}

TEST_F(SegmentLogTest, PartialWriteRollsBackAndMarksRecoveryRequired) {
  SegmentLogOptions options;
  // Inject failure after 2 records in a 3-record batch
  options.inject_write_failure_after_records = 2;

  SegmentLog log(directory_, 0, options);
  log.recover();

  std::vector<Record> records;
  records.push_back(Record{.value = {std::byte{1}}});
  records.push_back(Record{.value = {std::byte{2}}});
  records.push_back(Record{.value = {std::byte{3}}});

  // The append must fail due to the injected fault
  EXPECT_THROW(log.append(records, AcknowledgementMode::kAppended), std::runtime_error);

  // The log must now be marked as requiring recovery
  EXPECT_TRUE(log.needs_recovery());

  // Subsequent append and fetch calls must be safely rejected to prevent offset reuse or inconsistent state
  EXPECT_THROW(log.append({Record{.value = {std::byte{99}}}}, AcknowledgementMode::kAppended), std::runtime_error);
  EXPECT_THROW(log.fetch(0, 1), std::runtime_error);

  // Clearing the fault injection and recovering must restore consistent state
  log.set_fault_injection_after_records(0);
  ASSERT_NO_THROW(log.recover());
  EXPECT_FALSE(log.needs_recovery());

  // New appends succeed, starting contiguously from offset 0
  auto res = log.append({Record{.value = {std::byte{10}}}}, AcknowledgementMode::kAppended);
  EXPECT_EQ(res.first_offset, 0U);
  EXPECT_EQ(res.last_offset, 0U);

  auto fetched = log.fetch(0, 1);
  ASSERT_EQ(fetched.size(), 1U);
  EXPECT_EQ(fetched[0].offset, 0U);
  EXPECT_EQ(fetched[0].value[0], std::byte{10});
}

}  // namespace
}  // namespace streamforge::storage

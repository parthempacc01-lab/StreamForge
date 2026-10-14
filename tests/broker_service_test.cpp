#include "streamforge/broker/broker_service.hpp"

#include <barrier>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include <gtest/gtest.h>

namespace streamforge::broker {
namespace {

class BrokerServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::filesystem::temp_directory_path() /
            ("streamforge-service-test-" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  void TearDown() override { std::filesystem::remove_all(root_); }
  std::filesystem::path root_;
};

TEST_F(BrokerServiceTest, ProducesFetchesAndRecovers) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("orders", 1);
  broker.produce("orders", 0, {storage::Record{.value = {std::byte{0x42}}}},
                  storage::AcknowledgementMode::kAppended);
  EXPECT_EQ(broker.fetch("orders", 0, 0, 10).records.size(), 1U);

  BrokerService restarted(root_);
  restarted.recover();
  EXPECT_EQ(restarted.fetch("orders", 0, 0, 10).next_offset, 1U);
}

TEST_F(BrokerServiceTest, PersistsCommittedConsumerOffsets) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("orders", 1);
  broker.commit_offset("reporting", "orders", 0, 17);
  EXPECT_EQ(broker.committed_offset("reporting", "orders", 0), 17U);

  BrokerService restarted(root_);
  restarted.recover();
  EXPECT_EQ(restarted.committed_offset("reporting", "orders", 0), 17U);
}

TEST_F(BrokerServiceTest, EnforcesConfiguredOperationalLimits) {
  BrokerService broker(root_, BrokerLimits{.maximum_partitions_per_topic = 1, .maximum_records_per_batch = 1, .maximum_fetch_records = 1});
  broker.recover();
  EXPECT_THROW(broker.create_topic("too-many", 2), std::invalid_argument);
  broker.create_topic("orders", 1);
  EXPECT_THROW(broker.produce("orders", 0, {}, storage::AcknowledgementMode::kAppended), std::invalid_argument);
  EXPECT_THROW(broker.fetch("orders", 0, 0, 2), std::invalid_argument);
}

TEST_F(BrokerServiceTest, RejectsMalformedDurableOffsetMetadataOnRestart) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("orders", 1);
  broker.commit_offset("reporting", "orders", 0, 3);
  { std::ofstream output(root_ / "metadata" / "offsets", std::ios::trunc);
    output << "STREAMFORGE_OFFSETS_V1\nreporting orders not-a-partition 3\n"; }

  BrokerService restarted(root_);
  EXPECT_THROW(restarted.recover(), std::runtime_error);
}

TEST_F(BrokerServiceTest, ConcurrentProducesKeepSamePartitionOffsetsContiguous) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("orders", 1);
  constexpr std::size_t kThreads = 8;
  constexpr std::size_t kRecordsPerThread = 20;
  std::barrier start(static_cast<std::ptrdiff_t>(kThreads));
  std::vector<std::thread> threads;
  for (std::size_t thread = 0; thread < kThreads; ++thread) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (std::size_t record = 0; record < kRecordsPerThread; ++record) {
        (void)broker.produce("orders", 0, {storage::Record{.value = {std::byte{0xA1}}}},
                             storage::AcknowledgementMode::kAppended);
      }
    });
  }
  for (auto& thread : threads) thread.join();
  const auto records = broker.fetch("orders", 0, 0, kThreads * kRecordsPerThread);
  ASSERT_EQ(records.records.size(), kThreads * kRecordsPerThread);
  for (std::size_t index = 0; index < records.records.size(); ++index) {
    EXPECT_EQ(records.records[index].offset, index);
  }
}

TEST_F(BrokerServiceTest, ConcurrentPartitionsAndOffsetCommitsRemainIndependent) {
  BrokerService broker(root_);
  broker.recover();
  constexpr std::size_t kPartitions = 4;
  constexpr std::size_t kRecords = 15;
  broker.create_topic("events", kPartitions);
  std::barrier start(static_cast<std::ptrdiff_t>(kPartitions));
  std::vector<std::thread> threads;
  for (std::size_t partition = 0; partition < kPartitions; ++partition) {
    threads.emplace_back([&, partition] {
      start.arrive_and_wait();
      for (std::size_t record = 0; record < kRecords; ++record) {
        (void)broker.produce("events", static_cast<std::uint32_t>(partition),
                             {storage::Record{.value = {std::byte{0xB2}}}},
                             storage::AcknowledgementMode::kAppended);
      }
      broker.commit_offset("group-" + std::to_string(partition), "events",
                           static_cast<std::uint32_t>(partition), kRecords);
    });
  }
  for (auto& thread : threads) thread.join();
  for (std::size_t partition = 0; partition < kPartitions; ++partition) {
    const auto records = broker.fetch("events", static_cast<std::uint32_t>(partition), 0, kRecords);
    ASSERT_EQ(records.records.size(), kRecords);
    for (std::size_t index = 0; index < kRecords; ++index) EXPECT_EQ(records.records[index].offset, index);
    EXPECT_EQ(broker.committed_offset("group-" + std::to_string(partition), "events",
                                      static_cast<std::uint32_t>(partition)), kRecords);
  }
}

TEST_F(BrokerServiceTest, ConcurrentProduceAndFetchReturnOnlyCompleteRecords) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("events", 1);
  constexpr std::size_t kRecords = 100;
  std::barrier start(2);
  std::atomic_bool producer_failed{false};
  std::atomic_bool fetcher_failed{false};
  std::thread producer([&] {
    start.arrive_and_wait();
    try {
      for (std::size_t index = 0; index < kRecords; ++index) {
        (void)broker.produce("events", 0, {storage::Record{.value = {std::byte{0xC3}}}},
                             storage::AcknowledgementMode::kAppended);
      }
    } catch (...) { producer_failed = true; }
  });
  std::thread fetcher([&] {
    start.arrive_and_wait();
    try {
      for (std::size_t iteration = 0; iteration < kRecords; ++iteration) {
        const auto fetched = broker.fetch("events", 0, 0, kRecords);
        for (std::size_t index = 0; index < fetched.records.size(); ++index) {
          if (fetched.records[index].offset != index) fetcher_failed = true;
        }
      }
    } catch (...) { fetcher_failed = true; }
  });
  producer.join();
  fetcher.join();
  EXPECT_FALSE(producer_failed.load());
  EXPECT_FALSE(fetcher_failed.load());
  const auto records = broker.fetch("events", 0, 0, kRecords);
  ASSERT_EQ(records.records.size(), kRecords);
  for (std::size_t index = 0; index < kRecords; ++index) EXPECT_EQ(records.records[index].offset, index);
}

TEST_F(BrokerServiceTest, ConcurrentTopicCreationHasOneWinnerAndUsablePartitions) {
  BrokerService broker(root_);
  broker.recover();
  constexpr std::size_t kThreads = 6;
  std::barrier start(static_cast<std::ptrdiff_t>(kThreads));
  std::atomic_size_t successful_creates{0};
  std::atomic_bool unexpected_failure{false};
  std::vector<std::thread> threads;
  for (std::size_t index = 0; index < kThreads; ++index) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      try {
        broker.create_topic("racing-topic", 2);
        ++successful_creates;
      } catch (const std::invalid_argument&) {
      } catch (...) { unexpected_failure = true; }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(successful_creates.load(), 1U);
  EXPECT_FALSE(unexpected_failure.load());
  (void)broker.produce("racing-topic", 1, {storage::Record{.value = {std::byte{0xD4}}}},
                       storage::AcknowledgementMode::kAppended);
  EXPECT_EQ(broker.fetch("racing-topic", 1, 0, 1).records.size(), 1U);
}

TEST_F(BrokerServiceTest, ConcurrentCommitsSerializeDurableOffsetUpdates) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("events", 1);
  constexpr std::size_t kThreads = 8;
  std::barrier start(static_cast<std::ptrdiff_t>(kThreads));
  std::atomic_bool failed{false};
  std::vector<std::thread> threads;
  for (std::size_t index = 0; index < kThreads; ++index) {
    threads.emplace_back([&, index] {
      start.arrive_and_wait();
      try { broker.commit_offset("group", "events", 0, index); }
      catch (...) { failed = true; }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_FALSE(failed.load());
  const auto committed = broker.committed_offset("group", "events", 0);
  ASSERT_TRUE(committed.has_value());
  EXPECT_LT(*committed, kThreads);
  BrokerService restarted(root_);
  restarted.recover();
  EXPECT_EQ(restarted.committed_offset("group", "events", 0), committed);
}

TEST_F(BrokerServiceTest, RejectsRecoveryAfterServiceOperationsBegin) {
  BrokerService broker(root_);
  broker.recover();
  broker.create_topic("orders", 1);
  EXPECT_THROW(broker.recover(), std::logic_error);
}

}  // namespace
}  // namespace streamforge::broker

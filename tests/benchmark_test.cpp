#include "streamforge/benchmark/benchmark.hpp"
#include <gtest/gtest.h>
namespace streamforge::benchmark {

TEST(Benchmark, CalculatesPercentiles) {
  EXPECT_DOUBLE_EQ(percentile({1, 2, 3, 4, 5}, .50), 3);
  EXPECT_DOUBLE_EQ(percentile({1, 2, 3, 4, 5}, .95), 4);
}

TEST(Benchmark, RejectsInvalidArguments) {
  char executable[] = "bench";
  char directory[] = "data";
  char batch_option[] = "--batch-size";
  char zero[] = "0";
  char* invalid_batch[] = {executable, directory, batch_option, zero};
  EXPECT_THROW(static_cast<void>(parse_options(4, invalid_batch)), std::invalid_argument);

  char clients_option[] = "--clients";
  char unsupported_clients[] = "3";
  char* invalid_clients[] = {executable, directory, clients_option, unsupported_clients};
  EXPECT_THROW(static_cast<void>(parse_options(4, invalid_clients)), std::invalid_argument);

  char supported_clients[] = "16";
  char* valid_clients[] = {executable, directory, clients_option, supported_clients};
  EXPECT_EQ(parse_options(4, valid_clients).clients, 16U);

  char partitions_option[] = "--partitions";
  char unsupported_partitions[] = "2";
  char* invalid_partitions[] = {executable, directory, partitions_option, unsupported_partitions};
  EXPECT_THROW(static_cast<void>(parse_options(4, invalid_partitions)), std::invalid_argument);

  char supported_partitions[] = "4";
  char* valid_partitions[] = {executable, directory, partitions_option, supported_partitions};
  EXPECT_EQ(parse_options(4, valid_partitions).partitions, 4U);
}

TEST(Benchmark, AssignsClientsAndMessagesToRequestedPartitions) {
  EXPECT_EQ(target_partition(0, 4), 0U);
  EXPECT_EQ(target_partition(5, 4), 1U);
  EXPECT_EQ(messages_per_partition(10, 4, 1), (std::vector<std::size_t>{10}));
  EXPECT_EQ(messages_per_partition(10, 4, 4), (std::vector<std::size_t>{3, 3, 2, 2}));
  EXPECT_THROW(static_cast<void>(target_partition(0, 0)), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(messages_per_partition(1, 0, 1)), std::invalid_argument);
}

}  // namespace streamforge::benchmark

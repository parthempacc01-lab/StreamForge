#include "streamforge/broker/topic_catalog.hpp"

#include <chrono>
#include <filesystem>

#include <gtest/gtest.h>

namespace streamforge::broker {
namespace {

class TopicCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::filesystem::temp_directory_path() /
            ("streamforge-catalog-test-" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  void TearDown() override { std::filesystem::remove_all(root_); }
  std::filesystem::path root_;
};

TEST_F(TopicCatalogTest, PersistsTopicsAcrossReload) {
  TopicCatalog catalog(root_);
  catalog.load();
  catalog.create_topic("orders", 3);
  TopicCatalog restarted(root_);
  restarted.load();
  EXPECT_EQ(restarted.describe("orders").partition_count, 3U);
}

TEST_F(TopicCatalogTest, RejectsUnsafeTopicNames) {
  TopicCatalog catalog(root_);
  catalog.load();
  EXPECT_THROW(catalog.create_topic("../escape", 1), std::invalid_argument);
}

}  // namespace
}  // namespace streamforge::broker

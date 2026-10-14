#include "streamforge/broker/replication_cluster.hpp"
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
namespace streamforge::broker { TEST(ReplicationCluster, CopiesLeaderWritesToFollower) { const auto root = std::filesystem::temp_directory_path() / ("streamforge-replica-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())); BrokerService leader(root / "leader"), follower(root / "follower"); leader.recover(); follower.recover(); ReplicationCluster cluster(leader); cluster.add_follower(follower); cluster.create_topic("events", 1); cluster.produce("events", 0, {storage::Record{.value={std::byte{0x42}}}}, storage::AcknowledgementMode::kAppended); EXPECT_EQ(follower.fetch("events",0,0,1).records.front().value, (std::vector<std::byte>{std::byte{0x42}})); std::filesystem::remove_all(root); } }

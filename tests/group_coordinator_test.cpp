#include "streamforge/broker/group_coordinator.hpp"
#include <gtest/gtest.h>
namespace streamforge::broker { TEST(GroupCoordinator, RebalancesAndRemovesMembers) { GroupCoordinator groups; EXPECT_EQ(groups.join("g", "a", 3).partitions, (std::vector<std::uint32_t>{0,1,2})); EXPECT_EQ(groups.join("g", "b", 3).partitions, (std::vector<std::uint32_t>{1})); EXPECT_EQ(groups.heartbeat("g", "a").partitions, (std::vector<std::uint32_t>{0,2})); groups.leave("g", "b"); EXPECT_EQ(groups.heartbeat("g", "a").partitions, (std::vector<std::uint32_t>{0,1,2})); } }

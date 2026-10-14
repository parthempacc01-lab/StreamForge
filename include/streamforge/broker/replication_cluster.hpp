#pragma once
#include "streamforge/broker/broker_service.hpp"
#include <functional>
namespace streamforge::broker {
// Synchronous leader/follower replication for a single process test cluster.
class ReplicationCluster { public:
  explicit ReplicationCluster(BrokerService& leader) : leader_(leader) {}
  void add_follower(BrokerService& follower) { followers_.push_back(std::ref(follower)); }
  void create_topic(const std::string&, std::uint32_t);
  storage::AppendResult produce(const std::string&, std::uint32_t, std::vector<storage::Record>, storage::AcknowledgementMode);
 private: BrokerService& leader_; std::vector<std::reference_wrapper<BrokerService>> followers_; };
}  // namespace streamforge::broker

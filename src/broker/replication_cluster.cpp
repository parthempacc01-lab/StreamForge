#include "streamforge/broker/replication_cluster.hpp"
namespace streamforge::broker {
void ReplicationCluster::create_topic(const std::string& name, std::uint32_t partitions) { leader_.create_topic(name, partitions); for (auto follower : followers_) follower.get().create_topic(name, partitions); }
storage::AppendResult ReplicationCluster::produce(const std::string& topic, std::uint32_t partition, std::vector<storage::Record> records, storage::AcknowledgementMode mode) { auto result = leader_.produce(topic, partition, records, mode); for (auto follower : followers_) (void)follower.get().produce(topic, partition, records, mode); return result; }
}  // namespace streamforge::broker

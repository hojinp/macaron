#include "consistent_hashing.hpp"

void ConsistentHashRing::addNode(const std::string& node) {
    for (int i = 0; i < replicas; ++i) {
        std::string node_key = node + "_" + std::to_string(i);
        int replica_key = hashId(node_key, CONSISTENT_HASH_SIZE);
        ring[replica_key] = node;
        sorted_keys.insert(replica_key);
    }
}

void ConsistentHashRing::removeNode(const std::string& node) {
    for (int i = 0; i < replicas; ++i) {
        std::string node_key = node + "_" + std::to_string(i);
        int replica_key = hashId(node_key, CONSISTENT_HASH_SIZE);
        ring.erase(replica_key);
        sorted_keys.erase(replica_key);
    }
}

std::string ConsistentHashRing::getNode(const std::string& key_) {
    if (ring.empty()) {
        return "";
    }

    std::string key = key_;
    int hash_value = hashId(key, CONSISTENT_HASH_SIZE);
    auto it = sorted_keys.lower_bound(hash_value);

    if (it == sorted_keys.end()) {
        it = sorted_keys.begin();
    }

    return ring[*it];
}
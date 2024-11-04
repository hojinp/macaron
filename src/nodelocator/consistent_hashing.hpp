#ifndef CONSISTENT_HASHING_HPP
#define CONSISTENT_HASHING_HPP

#include <bits/stdc++.h>
#include <map>
#include <set>
#include "utils.hpp"
#include "configs.hpp"

class ConsistentHashRing {
private:
    std::map<int, std::string> ring;
    std::set<int> sorted_keys;
    int replicas;
 
public:
    ConsistentHashRing(int replicas = 28) : replicas(replicas) {}
 
    void addNode(const std::string& node);
 
    void removeNode(const std::string& node);
 
    std::string getNode(const std::string& key_);
};

#endif // CONSISTENT_HASHING_HPP

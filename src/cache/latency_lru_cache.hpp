#ifndef LATENCY_LRUCACHE_HPP
#define LATENCY_LRUCACHE_HPP

#include <vector>
#include <string>
#include <rocksdb/db.h>

class LatencyLRUCache {
public:
    LatencyLRUCache(const std::string& cacheName_, long long cacheSize_, rocksdb::DB* db_);

    long long get(std::string& key);
    void put(std::string& key, long long value, long long birth);
    long long del(std::string& key);

    long long getBirth(std::string& key);
    void updateCacheSize(long long cacheSize_);

private:
    void encodeKey(std::string key, std::string& newKey);
    void decodeKey(std::string encodedKey, std::string& key);
    void infoToValue(std::string& value, std::string& prv, std::string& nxt, const long long& size, const long long& birth);
    void valueToInfo(const std::string& value, std::string& prv, std::string& nxt, long long& size, long long& birth);

    std::string nullKey = "NULLKEY";
    std::string head = nullKey;
    std::string tail = nullKey;

    std::string cacheName;
    long long cacheSize;
    long long occupiedSize = 0LL;
    rocksdb::DB* db;
};

#endif // LATENCY_LRUCACHE_HPP
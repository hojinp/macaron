#ifndef LATENCY_LRUCACHELAMBDA_HPP
#define LATENCY_LRUCACHELAMBDA_HPP

#include <vector>
#include <string>
#include <rocksdb/db.h>

class LatencyLRUCacheLambda {
public:
    LatencyLRUCacheLambda(long long cacheSize_, rocksdb::DB* db_);

    long long get(std::string& key);
    void put(std::string& key, long long value, long long birth);
    long long del(std::string& key);

    long long getBirth(std::string& key);
    void updateCacheSize(long long cacheSize_);
    
    void saveState(std::string& sizeFilePath);
    void loadState(std::string& sizeFilePath);

private:
    void infoToValue(std::string& value, std::string& prv, std::string& nxt, const long long& size, const long long& birth);
    void valueToInfo(const std::string& value, std::string& prv, std::string& nxt, long long& size, long long& birth);

    std::string nullKey = "NULLKEY";
    std::string head = nullKey;
    std::string tail = nullKey;

    long long cacheSize;
    long long occupiedSize = 0LL;
    rocksdb::DB* db;
};

#endif // LATENCY_LRUCACHELAMBDA_HPP
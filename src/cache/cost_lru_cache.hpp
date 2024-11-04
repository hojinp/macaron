#ifndef COSTLRUCACHE_HPP
#define COSTLRUCACHE_HPP

#include <vector>
#include <string>
#include <rocksdb/db.h>

class CostLRUCache {
public:
    CostLRUCache(const std::string& cacheName_, long long cacheSize_, rocksdb::DB* db_);

    long long get(std::string& key);
    void put(std::string& key, long long value);
    void del(std::string& key);

    void addBytesMiss(long long size);
    void getTmpHitMiss(int& hit_, int& miss_);
    void getHitMiss(int& hit_, int& miss_);
    void getTmpBytesMiss(long long& tmpBytesMiss_);
    void resetTmps();

private:
    void encodeKey(std::string key, std::string& newKey);
    void decodeKey(std::string encodedKey, std::string& key);
    void infoToValue(std::string& value, std::string& prv, std::string& nxt, const std::string& size);
    void valueToInfo(const std::string& value, char delimiter, std::string result[3]);
    void markHit();
    void markMiss();

    std::string nullKey = "NULLKEY";
    char valueDelimiter = ',';
    std::string head = nullKey;
    std::string tail = nullKey;

    std::string cacheName;
    long long cacheSize;
    long long occupiedSize = 0LL;
    rocksdb::DB* db;

    long long tmpBytesMiss = 0LL;
    int hit = 0;
    int miss = 0;
    int tmpHit = 0;
    int tmpMiss = 0;
};

#endif
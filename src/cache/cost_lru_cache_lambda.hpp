#ifndef COSTLRUCACHELAMBDA_HPP
#define COSTLRUCACHELAMBDA_HPP

#include <vector>
#include <string>
#include <rocksdb/db.h>

class CostLRUCacheLambda {
public:
    CostLRUCacheLambda(long long cacheSize_, rocksdb::DB* db_);

    long long get(std::string key);
    void put(std::string key, long long value);
    void del(std::string key);

    void addBytesMiss(long long size);
    void getTmpHitMiss(int& hit_, int& miss_);
    void getHitMiss(int& hit_, int& miss_);
    void getTmpBytesMiss(long long& tmpBytesMiss_);
    void resetTmps();
    
    void saveState(std::string& sizeFilePath);
    void loadState(std::string& sizeFilePath);

private:
    std::string infoToValue(std::string prv, std::string nxt, std::string size);
    void valueToInfo(std::string value, std::vector<std::string>& results);
    void markHit();
    void markMiss();

    std::string nullKey = "NULLKEY";
    char valueDelimiter = ',';
    std::string head = nullKey;
    std::string tail = nullKey;

    long long cacheSize;
    long long occupiedSize = 0LL;
    rocksdb::DB* db;

    long long tmpBytesMiss = 0LL;
    int hit = 0;
    int miss = 0;
    int tmpHit = 0;
    int tmpMiss = 0;
};

#endif // COSTLRUCACHELAMBDA_HPP
#ifndef COST_MINISIM_H
#define COST_MINISIM_H 

#include <vector>
#include <unordered_map>
#include <string>
#include <rocksdb/db.h> 

#include "cache_engine_logger.hpp"
#include "cost_lru_cache.hpp"

class CostMiniatureSimulation {
public:
    CostMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitBM_);
    CostMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_, std::string dbBaseDirPath_);
    
    void loadLastLambdaData(std::string baseDirPath, long long minute_);
    
    void run(long long minute, std::vector<LogEntry*>& logEntries);

    void getCacheSizeMBs(int cacheSizeMBs[]);
    void getMRC(double mrcs[]);
    void getBMC(long long bytesMisses_[]);
    void getReqCounts(int& numGets, int& numPuts);

private:
    double samplingRatio;

    int miniCacheCount;
    int cacheSizeUnitMB;

    std::unordered_map<int, CostLRUCache*> caches;

    rocksdb::DB* db;

    std::vector<int> minutes;
    std::vector<std::vector<int>*> hits;
    std::vector<std::vector<int>*> misses;
    std::vector<std::vector<long long>*> bytesMisses;
    std::vector<int> sampledGetCounts;
    std::vector<int> sampledPutCounts;

    std::string statFilePath;
    std::string reqCountFilePath;
    std::string debugFilePath;
};

#endif // COST_MINISIM_H

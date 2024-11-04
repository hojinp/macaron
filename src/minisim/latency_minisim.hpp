#ifndef LATENCY_MINISIM_H
#define LATENCY_MINISIM_H 

#include <vector>
#include <unordered_map>
#include <string>
#include <rocksdb/db.h>

#include "cache_engine_logger.hpp"
#include "latency_lru_cache.hpp"
#include "latency_generator.hpp"

class LatencyMiniatureSimulation {
public:
    LatencyMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_);
    LatencyMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_, long long oscSizeByte, std::string dbBaseDirPath_);
    
    void loadLastLambdaData(std::string baseDirPath, long long minute_);
    
    void run(long long minute, std::vector<LogEntry*>& logEntries);

    void updateOSCSize(long long oscSizeByte);
    void getCacheSizeMBs(int cacheSizeMBs[]);
    void getLatencies(long long latencies[]);
    void getRequestCount(int& latencyRequestCount);
    void getSampledGetCount(int& sampledGetCount);

private:
    double samplingRatio;
    int sampledHashSpace;

    int miniCacheCount;
    int cacheSizeUnitMB;

    LatencyGenerator* latencyGenerator;

    std::unordered_map<int, LatencyLRUCache*> dramCaches;
    std::unordered_map<int, LatencyLRUCache*> oscCaches;

    rocksdb::DB* db;

    std::vector<int> minutes;
    std::vector<int> sampledGetCounts;
    std::vector<int> totalRequestCounts;
    std::vector<std::vector<long long>*> latencies;

    std::string statFilePath;
    std::string reqCountFilePath;
};

#endif // LATENCY_MINISIM_H

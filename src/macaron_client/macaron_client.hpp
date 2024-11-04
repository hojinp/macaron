#ifndef MACARONCLIENT_HPP
#define MACARONCLIENT_HPP

#include <string>
#include <vector>
#include <map>
#include <grpcpp/grpcpp.h>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "consistent_hashing.hpp"
#include "cache_engine.grpc.pb.h"
#include "mbakery.grpc.pb.h"
#include "configs.hpp"

struct LatencyLog {
    std::string key;
    long long latency;
};

class MacaronClient {
public:
    MacaronClient();

    void connect(const std::string& mbakery_ipaddress);

    void Put(const std::string& key, const std::string& data);

    bool Get(const std::string& key, std::string& response_data);

    void Delete(const std::string& key);

    void TriggerOptimization();

    void StopOptimization();

    void addLatencyLog(std::string key, long long latency, int src);

    void writeLatencyLogs(std::string latencyLogFilePath);

    void startLoggingLatency(int seconds);

    void waitLatencyLogging();

    void enableLatencyLogging();

    void disableLatencyLogging();

    void flushCacheEngineLatencyLog(std::string latencyLogFilePath);

    void clearDRAMCache();

private:
    void checkCacheEngineCluster();

    std::string debugFilePath;

    bool latencyLoggingEnabled;
    std::vector<std::pair<long long, int>>* latencyLogs; // latency and src

    boost::shared_mutex rwlock;
    boost::shared_mutex loglock;

    std::thread latencyLoggingThread;

    std::atomic<int> loggingCount;

    std::vector<std::string> *cacheEngineAddresses;
    ConsistentHashRing *nodeLocator;

    std::string cacheEngineAddressesString;
    std::thread checkCacheEngineClusterThread;
    
    std::shared_ptr<grpc::Channel> mbakeryChannel;
    std::unique_ptr<mbakery::MBakeryService::Stub> mbakeryStub;

    std::map<std::string, std::shared_ptr<grpc::Channel>> *cacheEngineChannels;
    std::map<std::string, std::unique_ptr<cacheengine::CacheEngineService::Stub>> *cacheEngineStubs;
};

#endif // MACARONCLIENT_HPP
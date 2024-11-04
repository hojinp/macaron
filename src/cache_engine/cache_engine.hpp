#ifndef CACHEENGINE_HPP
#define CACHEENGINE_HPP

#include <string>
#include <memory>
#include <atomic>
#include <grpcpp/grpcpp.h>

#include "enums.hpp"
#include "cloudapis.hpp"
#include "redisapis.hpp"
#include "packing_block_manager.hpp"
#include "cache_engine_logger.hpp"
#include "cache_engine.grpc.pb.h"
#include "meringue.grpc.pb.h"

struct LatencyLog {
    std::string key;
    int src; // 0: dram, 1: osc, 2: dl
    long long size;
    long long dram;
    long long oscm;
    long long osc;
    long long dl;
    long long total;
};

class CacheEngineServiceImpl final : public cacheengine::CacheEngineService::Service {
public:
    CacheEngineServiceImpl();

    void start_redis_server(const std::string& confFilePath);

    grpc::Status Initialize(grpc::ServerContext* context, const cacheengine::InitializeRequest* request, cacheengine::InitializeResponse* response) override;

    grpc::Status Put(grpc::ServerContext* context, const cacheengine::PutRequest* request, cacheengine::PutResponse* response) override;

    grpc::Status Get(grpc::ServerContext* context, const cacheengine::GetRequest* request, cacheengine::GetResponse* response) override;

    grpc::Status Delete(grpc::ServerContext* context, const cacheengine::DeleteRequest* request, cacheengine::DeleteResponse* response) override;

    grpc::Status FlushLog(grpc::ServerContext* context, const cacheengine::FlushLogRequest* request, cacheengine::FlushLogResponse* response) override;

    grpc::Status FlushLatencyLog(grpc::ServerContext* context, const cacheengine::FlushLatencyLogRequest* request, cacheengine::FlushLatencyLogResponse* response) override;

    grpc::Status ClearDRAMCache(grpc::ServerContext* context, const cacheengine::ClearDRAMCacheRequest* request, cacheengine::ClearDRAMCacheResponse* response) override;

    grpc::Status Prefetch(grpc::ServerContext* context, const cacheengine::PrefetchRequest* request, cacheengine::PrefetchResponse* response) override;

    grpc::Status Stop(grpc::ServerContext* context, const cacheengine::StopRequest* request, cacheengine::StopResponse* response) override;

private:
    void putDL(const std::string& key, const std::string& data);
    void putOSC(const std::string& key, const std::string& data);
    void putOSCMD(const std::string& key, const size_t length);
    void putDRAM(std::string key, std::string data);

    bool getDL(const std::string& key, std::string& response_data);
    bool getOSCNoPacking(const std::string& key, std::string& response_data, long long& meringue, long long& osc);
    bool getOSCWithPacking(const std::string& key, std::string& response_data, long long& meringue, long long& osc);
    bool getOSCMDNoPacking(const std::string& key, long long& length);
    bool getOSCMDWithPacking(const std::string& key, std::string& blockId, int& offset, long long& length);
    bool getDRAM(const std::string& key, std::string& response_data);

    void deleteDL(const std::string& key);
    void deleteOSCNoPacking(const std::string& key);
    void deleteOSCWithPacking(const std::string& key);
    void deleteOSCMD(const std::string& key);
    void deleteDRAM(const std::string& key);

    bool getAndSetIsDLLoading(std::string key);
    void deleteIsDLLoading(std::string key);
    bool checkIsStillDLLoading(std::string key);

    void addLatencyLog(std::string key, int src, long long size, long long dram, long long oscm, long long osc, long long dl, long long total);

    void resetStats();

    long long getDRAMRemainingMemory();

    CloudServiceProvider dlCSP;
    std::string dlBucketName;
    CloudClient dlClient;

    CloudServiceProvider oscCSP;
    std::string oscBucketName;
    CloudClient oscClient;

    RedisClient redisClient;

    std::unordered_map<std::string, bool> keyToIsDLLoading;
    std::mutex dlLoadingLock;

    std::string meringueAddress;
    std::shared_ptr<grpc::Channel> meringueChannel;
    std::unique_ptr<meringue::MeringueService::Stub> meringueStub;

    CacheLevelMode cacheLevelMode;

    PackingMode packingMode;
    PackingBlockManager *packingBlockManager;

    std::string logFilePath;

    CacheEngineLogger *cacheEngineLogger;
    
    bool latency_logging_enabled;
    std::mutex latencyLogLock;
    std::vector<LatencyLog> *latencyLogs;

    std::atomic<int> oscPutCnt;
    std::atomic<long long> dlGetBytes;
    std::atomic<long long> dlPutBytes;
};


void serve();

#endif // CACHEENGINE_HPP
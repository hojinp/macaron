#ifndef MERINGUE_HPP
#define MERINGUE_HPP

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <rocksdb/db.h>
#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include <set>
#include <atomic>

#include "meringue.grpc.pb.h"
#include "meringue_cache.hpp"
#include "cache_item.hpp"
#include "cloudapis.hpp"
#include "enums.hpp"

class MeringueServiceImpl final : public meringue::MeringueService::Service {
public:
    MeringueServiceImpl();

    grpc::Status Initialize(grpc::ServerContext* context, const meringue::InitializeRequest* request, meringue::InitializeResponse* response) override;

    grpc::Status PutSingleMD(grpc::ServerContext* context, const meringue::PutSingleMDRequest* request, meringue::PutSingleMDResponse* response) override;

    grpc::Status GetSingleMD(grpc::ServerContext* context, const meringue::GetSingleMDRequest* request, meringue::GetSingleMDResponse* response) override;

    grpc::Status DeleteSingleMD(grpc::ServerContext* context, const meringue::DeleteSingleMDRequest* request, meringue::DeleteSingleMDResponse* response) override;

    grpc::Status UpdateCache(grpc::ServerContext* context, const meringue::UpdateCacheRequest* request, meringue::UpdateCacheResponse* response) override;

    grpc::Status UpdateSizeAndEvict(grpc::ServerContext* context, const meringue::EvictionRequest* request, meringue::EvictionResponse* response) override;

    grpc::Status RunGC(grpc::ServerContext* context, const meringue::GCRequest* request, meringue::GCResponse* response) override;

    grpc::Status SortAndWrite(grpc::ServerContext* context, const meringue::SortAndWriteRequest* request, meringue::SortAndWriteResponse* response) override;

private:
    void restoreEvictedItemIfNeeded(std::string blockId, CacheItem* cacheItem);
    void deprecate_item(std::string key, ObjectState state);
    void run_gc();
    std::string getNextBlockId();

    CacheAlgorithm cacheAlgorithm;
    std::string dbPath;
    PackingMode packingMode;

    rocksdb::DB* metadata;

    double gcThreshold;
    std::vector<std::string> evictedItems;
    std::set<std::string> gcBlockIdSet;
    int meringueBlockId = 0;

    rocksdb::DB* keyToBlockId;
    rocksdb::DB* keyToState;
    rocksdb::DB* keyToBlockInfo;
    rocksdb::DB* blockIdToOccupiedSize;
    rocksdb::DB* blockIdToSize;

    std::unordered_map<std::string, std::string> keyToBlockIdMem;
    std::unordered_map<std::string, std::vector<CacheItem*>*> blockIdToCacheItems;
    std::unordered_map<std::string, long long> blockIdToOccupiedSizeMem;
    std::unordered_map<std::string, long long> blockIdToSizeMem;
    std::unordered_map<std::string, std::string> oldToNewBlockId;

    boost::shared_mutex rwlock;

    MeringueCache* cache;
    std::string logFilePath;
    std::string sortFilePath;

    std::atomic<int> oscGCGetCount;
    std::atomic<int> oscGCPutCount;
    std::atomic<long long> totalDataSizeBytes;

    CloudClient* oscClient;
    std::string oscBucketName;
};


void serve();

#endif // MERINGUE_HPP
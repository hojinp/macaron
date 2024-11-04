#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <filesystem>
#include <string>
#include <thread>
#include <mutex>
#include <fstream>
#include <unordered_set>

#include "consistent_hashing.hpp"
#include "cache_engine.hpp"
#include "cache_engine.grpc.pb.h"
#include "configs.hpp"
#include "enums.hpp"
#include "meringue.grpc.pb.h"
#include "utils.hpp"

std::string NAME = "CacheEngine";
Logger* logger;

CacheEngineServiceImpl::CacheEngineServiceImpl() {
    resetStats();
}

void CacheEngineServiceImpl::resetStats() {
    oscPutCnt = 0;
    dlGetBytes = 0LL;
    dlPutBytes = 0LL;
}

grpc::Status CacheEngineServiceImpl::Initialize(grpc::ServerContext* context, const cacheengine::InitializeRequest* request, cacheengine::InitializeResponse* response) {
    logger->log_message(NAME, "Received Initialize request");

    dlCSP = static_cast<CloudServiceProvider>(request->dl_csp());
    dlBucketName = request->dl_bucket_name();
    if (request->is_on_prem()) {
        std::string remote_endpoint_url = request->remote_endpoint_url();
        dlClient.ConnectS3OnPrem(remote_endpoint_url);
    } else {
        std::string remote_region = request->remote_region();
        dlClient.ConnectS3Cloud(remote_region);
    }

    oscCSP = static_cast<CloudServiceProvider>(request->osc_csp());
    oscBucketName = request->osc_bucket_name();
    if (request->is_on_prem()) {
        std::string local_endpoint_url = request->local_endpoint_url();
        oscClient.ConnectS3OnPrem(local_endpoint_url);
    } else {
        std::string local_region = request->local_region();
        oscClient.ConnectS3Cloud(local_region);
    }


    meringueAddress = request->meringue_address();
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    channel_args.SetMaxSendMessageSize(64 * 1024 * 1024);
    meringueChannel = grpc::CreateCustomChannel(meringueAddress, grpc::InsecureChannelCredentials(), channel_args);
    meringueStub = meringue::MeringueService::NewStub(meringueChannel);

    cacheLevelMode = static_cast<CacheLevelMode>(request->cache_level_mode());
    CACHE_LEVEL_MODE = cacheLevelMode;
    logger->log_message(NAME, "Cache level mode: " + std::to_string(cacheLevelMode));
    if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
        start_redis_server(request->redis_conf_file_path());
    }

    logger->log_message(NAME, "Packing mode: " + std::to_string(request->packing_mode()));
    packingMode = static_cast<PackingMode>(request->packing_mode());
    if (packingMode == PackingMode::WITH_PACKING) {
        packingBlockManager = new PackingBlockManager(request->name(), request->max_block_size(), request->max_block_object_count(), &oscClient, oscBucketName, meringueAddress);
    }

    logFilePath = request->log_file_path();
    cacheEngineLogger = new CacheEngineLogger(logFilePath);

    latency_logging_enabled = request->latency_logging_enabled();
    if (latency_logging_enabled) {
        latencyLogs = new std::vector<LatencyLog>();
    }

    response->set_is_running(true);
    return grpc::Status::OK;
}

void CacheEngineServiceImpl::start_redis_server(const std::string& confFilePath) {
    logger->log_message(NAME, "Starting Redis server");
    std::string command = "/bin/redis-server " + confFilePath;
    int result = std::system(command.c_str());
    if (result != 0) {
        throw std::runtime_error("Failed to start Redis server");
    }

    logger->log_message(NAME, "Connecting to Redis server");
    redisClient.connect("127.0.0.1", 6379);
}

grpc::Status CacheEngineServiceImpl::Put(grpc::ServerContext* context, const cacheengine::PutRequest* request, cacheengine::PutResponse* response) {
    cacheEngineLogger->log_request(getCurrentTimeMillis(), CloudOperation::PUT, request->key(), request->data().size());
    if (packingMode == PackingMode::NO_PACKING) {
        std::thread dlThread([this, request]() { putDL(request->key(), request->data()); });
        std::thread oscThread([this, request]() { putOSC(request->key(), request->data()); });
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
            std::thread dramThread([this, request]() { putDRAM(request->key(), request->data()); });
            dramThread.join();
        }
        dlThread.join();
        oscThread.join();
    } else if (packingMode == PackingMode::WITH_PACKING) {
        std::thread dlThread([this, request]() { putDL(request->key(), request->data()); });
        std::thread dramThread;
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
            dramThread = std::thread([this, request]() { putDRAM(request->key(), request->data()); });
        }
        packingBlockManager->addData(request->key(), request->data());
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
            dramThread.join();
        }
        dlThread.join();
    } else {
        throw std::runtime_error("Unsupported packing mode: " + std::to_string(packingMode));
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::Get(grpc::ServerContext* context, const cacheengine::GetRequest* request, cacheengine::GetResponse* response) {
    std::string responseData;
    bool exists = false;
    
    int src = -1;
    long long dram = 0LL, oscm = 0LL, osc = 0LL, dl = 0LL, total = 0LL;
    auto start = std::chrono::high_resolution_clock::now();

    if (packingMode == PackingMode::NO_PACKING) {
        // Get data from DRAM if it exists
        auto dram_start = std::chrono::high_resolution_clock::now();
        exists = getDRAM(request->key(), responseData);
        dram = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - dram_start).count();
        if (exists) {
            src = 0;
            goto getdone;
        }

        // Get data from OSC if it exists
        exists = getOSCNoPacking(request->key(), responseData, oscm, osc);
        if (exists) {
            if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
                std::string copyKey = request->key();
                std::string copyData = responseData;
                std::thread(&CacheEngineServiceImpl::putDRAM, this, std::move(copyKey), std::move(copyData)).detach();
            }
            src = 1;
            goto getdone;
        }

        // Get data from DL if it exists
        bool isloading = getAndSetIsDLLoading(request->key());
        src = 2;
        if (!isloading) {
            auto dl_start = std::chrono::high_resolution_clock::now();
            exists = getDL(request->key(), responseData);
            dl = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - dl_start).count();

            if (exists) {
                if (cacheLevelMode == CacheLevelMode::TWO_LEVEL)
                    putDRAM(request->key(), responseData);
                std::string copyKey = request->key();
                std::string copyData = responseData;
                std::thread(&CacheEngineServiceImpl::putOSC, this, std::move(copyKey), std::move(copyData)).detach();
            }
            deleteIsDLLoading(request->key());
        } else {
            while (checkIsStillDLLoading(request->key()))
                std::this_thread::sleep_for(std::chrono::milliseconds(5));

            if (cacheLevelMode == CacheLevelMode::TWO_LEVEL)
                exists = getDRAM(request->key(), responseData);
            if (exists)
                goto getdone;

            exists = getOSCNoPacking(request->key(), responseData, oscm, osc);
        }
    } else if (packingMode == PackingMode::WITH_PACKING) {
        // Get data from packing buffer if it exists
        if (cacheLevelMode == CacheLevelMode::SINGLE_LEVEL) {
            exists = packingBlockManager->getIfDataExists(request->key(), responseData);
            if (exists) {
                src = 0;
                goto getdone;
            }
        }

        // Get data from DRAM if it exists
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
            auto dram_start = std::chrono::high_resolution_clock::now();
            exists = getDRAM(request->key(), responseData);
            dram = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - dram_start).count();
        }
        if (exists) {
            src = 0;
            goto getdone;
        }

        // Get data from OSC if it exists
        exists = getOSCWithPacking(request->key(), responseData, oscm, osc);
        if (exists) {
            if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
                std::string copyKey = request->key();
                std::string copyData = responseData;
                std::thread(&CacheEngineServiceImpl::putDRAM, this, std::move(copyKey), std::move(copyData)).detach();
            }
            src = 1;
            goto getdone;
        }

        // Get data from DL if it exists
        bool isloading = getAndSetIsDLLoading(request->key());
        src = 2;
        if (!isloading) {
            auto dl_start = std::chrono::high_resolution_clock::now();
            exists = getDL(request->key(), responseData);
            dl = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - dl_start).count();

            if (exists) {
                if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
                    std::string copyKey = request->key();
                    std::string copyData = responseData;
                    std::thread(&CacheEngineServiceImpl::putDRAM, this, std::move(copyKey), std::move(copyData)).detach();
                }
                packingBlockManager->addData(request->key(), responseData);
            }
            deleteIsDLLoading(request->key());
        } else {
            while (checkIsStillDLLoading(request->key()))
                std::this_thread::sleep_for(std::chrono::milliseconds(5));

            exists = packingBlockManager->getIfDataExists(request->key(), responseData);
            if (exists)
                goto getdone;

            if (cacheLevelMode == CacheLevelMode::TWO_LEVEL)
                exists = getDRAM(request->key(), responseData);
            if (exists)
                goto getdone;

            exists = getOSCWithPacking(request->key(), responseData, oscm, osc);
        }
    } else {
        throw std::runtime_error("Unsupported packing mode: " + std::to_string(packingMode));
    }

getdone:
    if (exists) {
        cacheEngineLogger->log_request(getCurrentTimeMillis(), CloudOperation::GET, request->key(), responseData.size());
        response->set_data(responseData);
        response->set_src(src);

        total = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count();
        if (latency_logging_enabled) {
            addLatencyLog(request->key(), src, responseData.size(), dram, oscm, osc, dl, total);
        }
    }
    response->set_exists(exists);
    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::Delete(grpc::ServerContext* context, const cacheengine::DeleteRequest* request, cacheengine::DeleteResponse* response) {
    cacheEngineLogger->log_request(getCurrentTimeMillis(), CloudOperation::DELETE, request->key(), 0);
    if (packingMode == PackingMode::NO_PACKING) {
        std::thread dlThread([this, request]() { deleteDL(request->key()); });
        std::thread oscThread([this, request]() { deleteOSCNoPacking(request->key()); });
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
            std::thread dramThread([this, request]() { deleteDRAM(request->key()); });
            dramThread.join();
        }
        dlThread.join();
        oscThread.join();
    } else if (packingMode == PackingMode::WITH_PACKING) {
        std::thread dlThread([this, request]() { deleteDL(request->key()); });
        std::thread oscThread([this, request]() { deleteOSCWithPacking(request->key()); });
        std::thread dramThread;
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL)
            dramThread = std::thread([this, request]() { deleteDRAM(request->key()); });
        packingBlockManager->deleteData(request->key());
        if (cacheLevelMode == CacheLevelMode::TWO_LEVEL)
            dramThread.join();
        dlThread.join();
        oscThread.join();
    } else {
        throw std::runtime_error("Unsupported packing mode: " + std::to_string(packingMode));
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::FlushLog(grpc::ServerContext* context, const cacheengine::FlushLogRequest* request, cacheengine::FlushLogResponse* response) {
    logger->log_message(NAME, "Received FlushLog request");

    // Flush request logs to local log files
    std::vector<std::string> logFilePaths;
    cacheEngineLogger->flush(logFilePaths);

    // Send log files to Meringue/Controller server
    logger->log_message(NAME, "Sending log files to Meringue/Controller server");
    std::string remoteDirPath = request->log_dir_path();
    std::string baseFileName = request->cache_name();
    for (std::string& logFilePath : logFilePaths) {
        std::string logFileBasename = std::filesystem::path(logFilePath).filename();
        std::filesystem::rename(logFilePath, remoteDirPath + "/" + baseFileName + "_" + logFileBasename);
    }
    response->set_file_count(logFilePaths.size());

    // Delete local log files
    logger->log_message(NAME, "Deleting local log files");
    for (std::string& logFilePath : logFilePaths) {
        std::filesystem::remove(logFilePath);
    }

    // Send statistics collected from this cache engine
    logger->log_message(NAME, "Sending statistics to Meringue/Controller server");
    if (packingMode == PackingMode::NO_PACKING) {
        response->set_osc_put_cnt(oscPutCnt);
    } else if (packingMode == PackingMode::WITH_PACKING) {
        response->set_osc_put_cnt(packingBlockManager->getOSCPutCnt());
        packingBlockManager->resetOSCPutCnt();
    } else {
        throw std::runtime_error("Unsupported packing mode: " + std::to_string(packingMode));
    }
    response->set_dl_get_bytes(dlGetBytes);
    response->set_dl_put_bytes(dlPutBytes);
    resetStats();

    response->set_success(true);
    return grpc::Status::OK;
}

void CacheEngineServiceImpl::putDL(const std::string& key, const std::string& data) {
    dlClient.PutData(dlBucketName, key, data);
    dlPutBytes += data.size();
}

void CacheEngineServiceImpl::putOSC(const std::string& key, const std::string& data) {
    oscClient.PutData(oscBucketName, key, data);
    putOSCMD(key, data.size());
    oscPutCnt++;
}

void CacheEngineServiceImpl::putOSCMD(const std::string& key, const size_t length) {
    meringue::PutSingleMDRequest request;
    meringue::PutSingleMDResponse response;
    grpc::ClientContext context;
    request.set_key(key);
    request.set_size(length);

    grpc::Status status = meringueStub->PutSingleMD(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to put metadata to Meringue");
    }
}

void CacheEngineServiceImpl::putDRAM(std::string key, std::string data) {
    redisClient.put(key, data);
}

bool CacheEngineServiceImpl::getDL(const std::string& key, std::string& responseData) {
    bool exists = dlClient.GetData(dlBucketName, key, responseData);
    if (exists) {
        dlGetBytes += responseData.size();
    }
    return exists;
}

bool CacheEngineServiceImpl::getOSCNoPacking(const std::string& key, std::string& responseData, long long& oscm, long long& osc) {
    long long length;

    auto oscm_start = std::chrono::high_resolution_clock::now();
    bool exists = getOSCMDNoPacking(key, length);
    oscm = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - oscm_start).count();

    if (!exists)
        return false;

    auto osc_start = std::chrono::high_resolution_clock::now();
    exists = oscClient.GetData(oscBucketName, key, responseData);
    osc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - osc_start).count();

    return exists;
}

bool CacheEngineServiceImpl::getOSCMDNoPacking(const std::string& key, long long& length) {
    meringue::GetSingleMDRequest request;
    meringue::GetSingleMDResponse response;
    grpc::ClientContext context;
    request.set_key(key);

    grpc::Status status = meringueStub->GetSingleMD(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to get metadata from Meringue");
    }

    bool exists = response.exists();
    if (exists) {
        length = response.size();
    }

    return exists;
}

bool CacheEngineServiceImpl::getOSCWithPacking(const std::string& key, std::string& responseData, long long& oscm, long long& osc) {
    std::string blockId;
    int offset;
    long long length;

    auto oscm_start = std::chrono::high_resolution_clock::now();
    bool exists = getOSCMDWithPacking(key, blockId, offset, length);
    oscm = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - oscm_start).count();

    if (!exists)
        return false;

    auto osc_start = std::chrono::high_resolution_clock::now();
    exists = oscClient.GetData(oscBucketName, blockId, offset, length, responseData);
    osc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - osc_start).count();

    return exists;
}

bool CacheEngineServiceImpl::getOSCMDWithPacking(const std::string& key, std::string& blockId, int& offset, long long& length) {
    meringue::GetSingleMDRequest request;
    meringue::GetSingleMDResponse response;
    grpc::ClientContext context;
    request.set_key(key);

    grpc::Status status = meringueStub->GetSingleMD(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to get metadata from Meringue");
    }

    bool exists = response.exists();
    if (exists) {
        blockId = response.block_id();
        offset = response.offset();
        length = response.size();
    }

    return exists;
}

bool CacheEngineServiceImpl::getDRAM(const std::string& key, std::string& responseData) {
    return redisClient.get(key, responseData);
}

void CacheEngineServiceImpl::deleteDL(const std::string& key) {
    dlClient.DeleteData(dlBucketName, key);
}

void CacheEngineServiceImpl::deleteOSCNoPacking(const std::string& key) {
    deleteOSCMD(key);
    oscClient.DeleteData(oscBucketName, key);
}

void CacheEngineServiceImpl::deleteOSCWithPacking(const std::string& key) {
    deleteOSCMD(key);
}

void CacheEngineServiceImpl::deleteOSCMD(const std::string& key) {
    meringue::DeleteSingleMDRequest request;
    meringue::DeleteSingleMDResponse response;
    grpc::ClientContext context;
    request.set_key(key);

    grpc::Status status = meringueStub->DeleteSingleMD(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete metadata from Meringue");
    }
}

void CacheEngineServiceImpl::deleteDRAM(const std::string& key) {
    redisClient.del(key);
}

void CacheEngineServiceImpl::addLatencyLog(std::string key, int src, long long size, long long dram, long long oscm, long long osc, long long dl, long long total) {
    LatencyLog log;
    log.key = key;
    log.src = src;
    log.size = size;
    log.dram = dram;
    log.oscm = oscm;
    log.osc = osc;
    log.dl = dl;
    log.total = total;
    std::lock_guard<std::mutex> lock(latencyLogLock);
    latencyLogs->push_back(log);
    // latencyLogs.push_back(log);
}

grpc::Status CacheEngineServiceImpl::FlushLatencyLog(grpc::ServerContext* context, const cacheengine::FlushLatencyLogRequest* request, cacheengine::FlushLatencyLogResponse* response) {
    logger->log_message(NAME, "Received FlushLatencyLog request");
    if (!latency_logging_enabled)
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Latency logging is not enabled");

    std::vector<LatencyLog> *oldLogs;
    {
        std::lock_guard<std::mutex> lock(latencyLogLock);
        oldLogs = latencyLogs;
        latencyLogs = new std::vector<LatencyLog>();
    }

    std::string logFilePath = request->log_file_path();
    std::filesystem::path path = logFilePath;
    if (!std::filesystem::exists(path.parent_path())) {
        std::filesystem::create_directories(path.parent_path());
    }

    std::ofstream latencyLogFile(logFilePath, std::ofstream::out);
    latencyLogFile << "key,src" << std::endl;
    for (auto log : *oldLogs)
        latencyLogFile << log.key << "," << log.src << std::endl;
    latencyLogFile.close();
    oldLogs->clear();
    delete oldLogs;

    logger->log_message(NAME, "Flushed latency log to " + logFilePath + " successfully.");
    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::ClearDRAMCache(grpc::ServerContext* context, const cacheengine::ClearDRAMCacheRequest* request, cacheengine::ClearDRAMCacheResponse* response) {
    logger->log_message(NAME, "Received ClearDRAMCache request");
    assert(cacheLevelMode == CacheLevelMode::TWO_LEVEL);
    
    std::string command = "redis-cli flushall";
    logger->log_message(NAME, "Clearing DRAM cache: " + command);
    int result = std::system(command.c_str());
    logger->log_message(NAME, "Result: " + std::to_string(result));
    if (result != 0) {
        logger->log_message(NAME, "Failed to clear DRAM cache");
        return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to clear DRAM cache");
    }
    logger->log_message(NAME, "DRAM cache cleared");

    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::Prefetch(grpc::ServerContext* context, const cacheengine::PrefetchRequest* request, cacheengine::PrefetchResponse* response) {
    logger->log_message(NAME, "Received Prefetch request");
    assert(cacheLevelMode == CacheLevelMode::TWO_LEVEL);

    bool newServer = request->new_server();

    std::string myAddress = request->target_address();
    std::string allCEAddresses = request->cache_engine_addresses();
    std::vector<std::string> addresses;
    splitString(allCEAddresses, addresses);
    ConsistentHashRing nodeLocator = ConsistentHashRing();
    logger->log_message(NAME, "New CE addresses: " + allCEAddresses);
    logger->log_message(NAME, "This CE address: " + myAddress);
    for (std::string& address : addresses) {
        nodeLocator.addNode(address);
    }

    if (!newServer) {
        std::unordered_set<std::string> keys;
        redisClient.scanAll(keys);
        for (std::string key : keys) {
            if (nodeLocator.getNode(key) != myAddress) {
                redisClient.del(key);
            }
        }
    }

    long long remainingMemory = getDRAMRemainingMemory();
    if (remainingMemory == -1LL) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to get remaining memory of DRAM");
    }
    remainingMemory = (long long) (0.95 * remainingMemory);
    logger->log_message(NAME, "Total bytes to be read: " + std::to_string(remainingMemory) + " (bytes)");

    std::vector<std::string> keysToPrefetch;
    std::string sortFilePath = request->sort_file_path();
    int totalCount = 0;
    while (true) {
        std::ifstream sortFile(sortFilePath, std::ios::binary);
        if (!sortFile.is_open()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        sortFile.seekg(0, std::ios::end);
        std::streampos filesize = sortFile.tellg();
        sortFile.seekg(0, std::ios::beg);
        
        char* buffer = new char[filesize];
        sortFile.read(buffer, filesize);

        size_t pos = 0;
        while (pos < filesize && remainingMemory > 0LL) {
            totalCount++;

            std::string key;
            size_t keySize;
            long long size;

            std::memcpy(&keySize, buffer + pos, sizeof(keySize));
            key.resize(keySize);
            std::memcpy(&key[0], buffer + pos + sizeof(keySize), keySize);
            std::memcpy(&size, buffer + pos + sizeof(keySize) + keySize, sizeof(size));

            std::string targetAddress = nodeLocator.getNode(key);
            if (targetAddress == myAddress) {
                keysToPrefetch.push_back(key);
                remainingMemory -= size;
            }

            pos += sizeof(keySize) + keySize + sizeof(size);
        }
        sortFile.close();
        delete[] buffer;
        break;
    }
    logger->log_message(NAME, "Total number of items in sorted file: " + std::to_string(totalCount));
    logger->log_message(NAME, "Number of items to be prefetched: "  + std::to_string(keysToPrefetch.size()));

    int nThreads = 32;
    std::vector<std::thread> threads;
    for (int i = keysToPrefetch.size() - 1; i >= 0; i--) {
        std::string key = keysToPrefetch[i];
        threads.push_back(std::thread([this, key]() {
            std::string data;
            long long oscm, osc;
            bool exists;
            if (packingMode == PackingMode::NO_PACKING) {
                exists = getOSCNoPacking(key, data, oscm, osc);
            } else {
                exists = getOSCWithPacking(key, data, oscm, osc);
            }
            if (exists) {
                putDRAM(key, data);
            }
        }));
        if (threads.size() == nThreads) {
            for (std::thread& t : threads) {
                t.join();
            }
            threads.clear();
        }
    }
    for (std::thread& t : threads) {
        t.join();
    }
    threads.clear();

    // XXX: Just for logging memory stats (can be deleted for performance)
    logger->log_message(NAME, "Done prefetching. Check the memory status.");
    getDRAMRemainingMemory();

    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status CacheEngineServiceImpl::Stop(grpc::ServerContext* context, const cacheengine::StopRequest* request, cacheengine::StopResponse* response) {
    logger->log_message(NAME, "Received Stop request");
    if (cacheLevelMode == CacheLevelMode::TWO_LEVEL) {
        packingBlockManager->flushBlock();
    }
    return grpc::Status::OK;
}

long long CacheEngineServiceImpl::getDRAMRemainingMemory() {
    FILE* pipe = popen("redis-cli INFO memory | grep used_memory:", "r");
    if (!pipe) {
        logger->log_message(NAME, "Failed to execute command: " + std::string(strerror(errno)));
        return -1LL;
    }
    char buffer1[128];
    std::string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer1, 128, pipe) != NULL)
            result += buffer1;
    }
    pclose(pipe);
    long long usedMemory = std::stoll(result.substr(12, result.size() - 12));

    pipe = popen("redis-cli INFO memory | grep maxmemory:", "r");
    if (!pipe) {
        logger->log_message(NAME, "Failed to execute command: " + std::string(strerror(errno)));
        return -1LL;
    }
    char buffer2[128];
    result = "";
    while (!feof(pipe)) {
        if (fgets(buffer2, 128, pipe) != NULL)
            result += buffer2;
    }
    pclose(pipe);
    long long maxMemory = std::stoll(result.substr(10, result.size() - 10));

    logger->log_message(NAME, "Used/Max memory: " + std::to_string(usedMemory) + "/" + std::to_string(maxMemory) + " (bytes)");

    return maxMemory - usedMemory;
}


bool CacheEngineServiceImpl::getAndSetIsDLLoading(std::string key) {
    std::lock_guard<std::mutex> lock(dlLoadingLock);
    auto it = keyToIsDLLoading.find(key);
    if (it == keyToIsDLLoading.end()) {
        it = keyToIsDLLoading.emplace(key, true).first;
        return false;
    }
    return true;
}

void CacheEngineServiceImpl::deleteIsDLLoading(std::string key) {
    std::lock_guard<std::mutex> lock(dlLoadingLock);
    keyToIsDLLoading.erase(key);
}

bool CacheEngineServiceImpl::checkIsStillDLLoading(std::string key) {
    std::lock_guard<std::mutex> lock(dlLoadingLock);
    auto it = keyToIsDLLoading.find(key);
    return it != keyToIsDLLoading.end();
}

void serve() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("[::]:50053", grpc::InsecureServerCredentials());
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, 64);
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);
    logger->log_message(NAME, "Server configs: NUM_CQS: 64, MaxReceiveMessageSize: 64MB, MaxSendMessageSize: 64MB");

    CacheEngineServiceImpl service;
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    logger->log_message(NAME, "Start listening on [::]:50053");
    server->Wait();
}

int main() {
    logger = new Logger(NAME);
    logger->log_message(NAME, "Starting CacheEngine");
    serve();
    return 0;
}
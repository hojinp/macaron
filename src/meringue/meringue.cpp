#include <thread>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include "rocksdbapis.hpp"
#include "configs.hpp"
#include "utils.hpp"
#include "enums.hpp"
#include "meringue.hpp"
#include "meringue.grpc.pb.h"
#include "meringue_cache.hpp"
#include "cache_engine_logger.hpp"

std::string NAME = "Meringue";
Logger* logger;


MeringueServiceImpl::MeringueServiceImpl() : cacheAlgorithm(CacheAlgorithm::LRU), packingMode(PACKING_MODE) {
    totalDataSizeBytes = 0LL;
    oscGCGetCount = 0;
    oscGCPutCount = 0;
}

grpc::Status MeringueServiceImpl::Initialize(grpc::ServerContext* context, const meringue::InitializeRequest* request, meringue::InitializeResponse* response) {
    logger->log_message(NAME, "Received Initialize request");
    packingMode = static_cast<PackingMode>(request->packing_mode());
    dbPath = request->meringue_db_path();
    if (packingMode == NO_PACKING) {
        metadata = rd_create_db(request->contd(), dbPath, "metadata.db");
    } else if (packingMode == WITH_PACKING) {
        gcThreshold = request->gc_threshold();
    } else {
        throw std::runtime_error("Invalid packing mode");
    }
    cache = new MeringueCache(INITIAL_OSC_SIZE_MB * 1024LL * 1024LL);
    oscClient = new CloudClient();
    if (request->is_on_prem()) {
        oscClient->ConnectS3OnPrem(request->local_endpoint_url());
    } else {
        oscClient->ConnectS3Cloud(request->local_region());
    }
    oscBucketName = request->osc_bucket_name();
    logFilePath = request->log_file_path();

    response->set_is_running(true);
    logger->log_message(NAME, "Send Initialize response");
    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::PutSingleMD(grpc::ServerContext* context, const meringue::PutSingleMDRequest* request, meringue::PutSingleMDResponse* response) {
    if (packingMode == NO_PACKING) {
        std::string key = request->key();
        size_t length = request->size();
        rd_put(metadata, key, std::to_string(length));
        response->set_success(true); // Example response
    } else if (packingMode == WITH_PACKING) {
        std::string blockId = request->key();
        int blockSize = request->size();
        std::string blockMapStr = request->block_map();
        std::stringstream ss(blockMapStr);
        std::string token;

        {
            boost::unique_lock<boost::shared_mutex> lock(rwlock);
            if (MERINGUE_INMEMORY_MODE) {
                blockIdToCacheItems[blockId] = new std::vector<CacheItem*>();
                long long validSize = 0LL;
                while (std::getline(ss, token, ',')) {
                    std::string key = token;
                    std::getline(ss, token, ',');
                    int offset = std::stoi(token);
                    std::getline(ss, token, ',');
                    long long length = std::stoll(token);
                    validSize += length;

                    deprecate_item(key, ObjectState::DELETED);

                    keyToBlockIdMem[key] = blockId;
                    blockIdToCacheItems[blockId]->push_back(new CacheItem(key, offset, length, ObjectState::VALID));
                }
                blockIdToOccupiedSizeMem[blockId] = validSize;
                blockIdToSizeMem[blockId] = blockSize;
                totalDataSizeBytes += blockSize;
            }
        }
    } else {
        throw std::runtime_error("Invalid packing mode");
    }

    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::GetSingleMD(grpc::ServerContext* context, const meringue::GetSingleMDRequest* request, meringue::GetSingleMDResponse* response) {
    if (packingMode == NO_PACKING) {
        std::string key = request->key();
        std::string value;
        bool found = rd_get(metadata, key, value);

        if (found) {
            response->set_exists(true);
            response->set_size(std::stoi(value));
        } else {
            response->set_exists(false);
        }
    } else if (packingMode == WITH_PACKING) {
        std::string key = request->key();
        bool found;
        {
            boost::upgrade_lock<boost::shared_mutex> lock(rwlock);
            if (MERINGUE_INMEMORY_MODE) {
                auto it = keyToBlockIdMem.find(key);
                found = (it != keyToBlockIdMem.end());
                if (found) {
                    std::string blockId = it->second;
                    std::vector<CacheItem*>* cacheItems = blockIdToCacheItems[it->second];
                    for (CacheItem* cacheItem: *cacheItems) {
                        if (cacheItem->key == key) {
                            response->set_block_id(blockId);
                            response->set_offset(cacheItem->offset);
                            response->set_size(cacheItem->size);
                            boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock(lock);
                            restoreEvictedItemIfNeeded(blockId, cacheItem);
                            break;
                        }
                    }
                }
            }
        }
        response->set_exists(found);
    } else {
        throw std::runtime_error("Invalid packing mode");
    }
    
    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::DeleteSingleMD(grpc::ServerContext* context, const meringue::DeleteSingleMDRequest* request, meringue::DeleteSingleMDResponse* response) {
    if (packingMode == NO_PACKING) {
        std::string key = request->key();
        rd_delete(metadata, key);
        response->set_success(true);
    } else if (packingMode == WITH_PACKING) {
        std::string key = request->key();
        {
            boost::unique_lock<boost::shared_mutex> lock(rwlock);
            if (MERINGUE_INMEMORY_MODE) {
                deprecate_item(key, ObjectState::DELETED);
            }
        }

        response->set_success(true);
    } else {
        throw std::runtime_error("Invalid packing mode");
    }
    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::UpdateCache(grpc::ServerContext* context, const meringue::UpdateCacheRequest* request, meringue::UpdateCacheResponse* response) {
    logger->log_message(NAME, "Start updating the cache size and evicting objects from the cache");
    std::mutex logMutex;
    std::vector<LogEntry*> logEntries;
    int nThreads = 32;
    std::vector<std::thread> threads;
    int logFileCount = request->log_file_count();
    while (true) {
        int count = 0;
        for (const auto& entry: std::filesystem::directory_iterator(logFilePath)) {
            count++;
        }
        if (count == logFileCount) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    for (const auto& entry: std::filesystem::directory_iterator(logFilePath)) {
        threads.emplace_back([&logEntries, &logMutex](const std::filesystem::directory_entry& entry) {
            if (entry.is_regular_file()) {
                std::vector<LogEntry*> localLogEntries;
                readCacheEngineLogFile(entry.path(), localLogEntries);

                std::lock_guard<std::mutex> lock(logMutex);
                logEntries.insert(logEntries.end(), localLogEntries.begin(), localLogEntries.end());
            }
        }, entry);

        if (threads.size() >= nThreads) {
            for (auto& thread : threads) {
                thread.join();
            }
            threads.clear();
        }
    }
    if (threads.size() > 0) {
        for (auto& thread : threads) {
            thread.join();
        }
    }
    threads.clear();

    if (logEntries.size() > 0) {
        std::sort(logEntries.begin(), logEntries.end(), [](LogEntry* a, LogEntry* b) {
            return a->timestamp < b->timestamp;
        });
    }

    // Simulate the cache operations according to the log entries
    logger->log_message(NAME, "Simulate the cache operations according to the log entries");
    for (LogEntry* entry: logEntries) {
        if (entry->opType == PUT) {
            cache->putWithoutEviction(entry->key, entry->size);
        } else if (entry->opType == GET) {
            long long objectSize = cache->get(entry->key);
            if (objectSize == -1L) {
                cache->putWithoutEviction(entry->key, entry->size);
            }
        } else if (entry->opType == DELETE) {
            cache->del(entry->key);
        } else {
            throw std::runtime_error("Invalid operation type");
        }
    }
    for (LogEntry* entry: logEntries) {
        delete entry;
    }
    logEntries.clear();
    
    logger->log_message(NAME, "Done simulating cache in Meringue");
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::UpdateSizeAndEvict(grpc::ServerContext* context, const meringue::EvictionRequest* request, meringue::EvictionResponse* response) {
    // Update the cache size and then logically evicts the objects from the cache
    logger->log_message(NAME, "Update the cache size and then logically evicts the objects from the cache");
    evictedItems.clear();
    long long prvOSCSizeBytes = cache->getOccupiedSize();
    long long newSizeMB = request->new_size_mb();
    cache->setNewCacheSize(newSizeMB * 1024LL * 1024LL);
    cache->evict(evictedItems);
    long long newOSCSizeBytes = cache->getOccupiedSize();
    response->set_prv_osc_size_bytes(prvOSCSizeBytes);
    response->set_new_osc_size_bytes(newOSCSizeBytes);
    response->set_success(true);

    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::RunGC(grpc::ServerContext* context, const meringue::GCRequest* request, meringue::GCResponse* response) {    
    // Logically/Physically deletes the evicted objects from the cache
    logger->log_message(NAME, "Logically/Physically deletes the evicted objects from the cache");
    int nThreads = 32;
    std::vector<std::thread> threads;
    if (packingMode == NO_PACKING) {
        for (std::string key: evictedItems) {
            threads.emplace_back([this, key]() {
                rd_delete(metadata, key);
                oscClient->DeleteData(oscBucketName, key);
            });
            if (threads.size() >= nThreads) {
                for (auto& thread : threads) {
                    thread.join();
                }
                threads.clear();
            }
        }
        if (threads.size() > 0) {
            for (auto& thread : threads) {
                thread.join();
            }
        }
        threads.clear();
    } else if (packingMode == WITH_PACKING) {
        response->set_prv_total_data_size_bytes(totalDataSizeBytes);
        if (MERINGUE_INMEMORY_MODE) {
            logger->log_message(NAME, "Number of items to be evicted: " + std::to_string(evictedItems.size()));
            {
                boost::unique_lock<boost::shared_mutex> lock(rwlock);
                for (std::string key: evictedItems) {
                    deprecate_item(key, ObjectState::EVICTED);
                }
            }
            run_gc();
        }
        response->set_new_total_data_size_bytes(totalDataSizeBytes);
        response->set_osc_gc_get_count(oscGCGetCount);
        response->set_osc_gc_put_count(oscGCPutCount);
        oscGCGetCount = 0;
        oscGCPutCount = 0;
    } else {
        throw std::runtime_error("Invalid packing mode");
    }
    evictedItems.clear();
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MeringueServiceImpl::SortAndWrite(grpc::ServerContext* context, const meringue::SortAndWriteRequest* request, meringue::SortAndWriteResponse* response) {
    long long sizeMB = request->size_mb();
    std::string filename = request->filename();
    std::filesystem::path p(filename);
    std::filesystem::create_directories(p.parent_path());

    logger->log_message(NAME, "Start sorting and writing the cache to the file");
    cache->sortAndWrite(filename, sizeMB);
    
    response->set_success(true);
    return grpc::Status::OK;
}


void MeringueServiceImpl::restoreEvictedItemIfNeeded(std::string blockId, CacheItem* cacheItem) {
    if (MERINGUE_INMEMORY_MODE) {
        assert(cacheItem->state != ObjectState::DELETED);
        if (cacheItem->state == ObjectState::EVICTED) {
            cacheItem->state = ObjectState::VALID;
            blockIdToOccupiedSizeMem[blockId] += cacheItem->size;
        }
    }
}

void MeringueServiceImpl::deprecate_item(std::string key, ObjectState state) {
    if (MERINGUE_INMEMORY_MODE) {
        auto it = keyToBlockIdMem.find(key);
        if (it == keyToBlockIdMem.end()) {
            return;
        }

        std::string blockId = it->second;
        long long itemSize = -1L;
        std::vector<CacheItem*>* cacheItems = blockIdToCacheItems[blockId];
        for (CacheItem* cacheItem: *cacheItems) {
            if (cacheItem->key == key) {
                itemSize = cacheItem->state == ObjectState::VALID ? cacheItem->size : 0L;
                cacheItem->state = state;
                break;
            }
        }

        if (state == ObjectState::DELETED) {
            keyToBlockIdMem.erase(key);
        }
        blockIdToOccupiedSizeMem[blockId] -= itemSize;
        if (blockIdToOccupiedSizeMem[blockId] < blockIdToSizeMem[blockId] * gcThreshold) {
            gcBlockIdSet.insert(blockId);
        }
    } else {
        throw new std::runtime_error("Packing mode is not supported yet");
    }
}

void MeringueServiceImpl::run_gc() {
    logger->log_message(NAME, "Start running garbage collection");
    if (MERINGUE_INMEMORY_MODE) {
        std::vector<std::string> blocksToBeGCed;
        {
            boost::unique_lock<boost::shared_mutex> lock(rwlock);
            for (std::string blockId : gcBlockIdSet) {
                if (blockIdToOccupiedSizeMem[blockId] > blockIdToSizeMem[blockId] * gcThreshold) {
                    continue;
                }

                std::vector<CacheItem*>* oldBlock = blockIdToCacheItems[blockId];
                std::vector<CacheItem*>* newBlock = new std::vector<CacheItem*>();
                long long newBlockSize = 0LL;
                for (CacheItem* cacheItem: *oldBlock) {
                    if (cacheItem->state == ObjectState::VALID) {
                        newBlock->push_back(new CacheItem(cacheItem->key, cacheItem->offset, cacheItem->size, ObjectState::VALID));
                        newBlockSize += cacheItem->size;
                    } else if (cacheItem->state == ObjectState::EVICTED) {
                        cacheItem->state = ObjectState::DELETED;
                        keyToBlockIdMem.erase(cacheItem->key);
                    }
                }

                if (newBlock->size() == 0) {
                    for (CacheItem* cacheItem: *oldBlock) {
                        delete cacheItem;
                    }
                    delete oldBlock;
                    delete newBlock;
                    blockIdToCacheItems.erase(blockId);
                    blockIdToOccupiedSizeMem.erase(blockId);
                    totalDataSizeBytes -= blockIdToSizeMem[blockId];
                    blockIdToSizeMem.erase(blockId);
                    std::thread([this, blockId]() { oscClient->DeleteData(oscBucketName, blockId); }).detach();
                    continue;
                }

                std::string newBlockId = getNextBlockId();
                blockIdToCacheItems[newBlockId] = newBlock;
                blockIdToOccupiedSizeMem[newBlockId] = newBlockSize;
                blockIdToSizeMem[newBlockId] = newBlockSize;
                totalDataSizeBytes += newBlockSize;

                oldToNewBlockId[blockId] = newBlockId;

                blocksToBeGCed.push_back(blockId);
            }
            gcBlockIdSet.clear();
        }

        logger->log_message(NAME, "Number of blocks to be GCed: " + std::to_string(blocksToBeGCed.size()));
        int nThreads = 32;
        std::vector<std::thread> threads;
        for (std::string oldBlockId : blocksToBeGCed) {
            threads.push_back(std::thread([this, oldBlockId]() {
                std::string oldBlockData;
                oscClient->GetData(oscBucketName, oldBlockId, oldBlockData);
                oscGCGetCount++;
                std::string newBlockId = oldToNewBlockId[oldBlockId];
                std::vector<CacheItem*>* newBlock;

                std::string buffer = "";
                {
                    boost::unique_lock<boost::shared_mutex> lock(rwlock);
                    newBlock = blockIdToCacheItems[newBlockId];
                    for (CacheItem* cacheItem: *newBlock) {
                        buffer += oldBlockData.substr(cacheItem->offset, cacheItem->size);
                    }
                }

                oscClient->PutData(oscBucketName, newBlockId, buffer);
                oscGCPutCount++;

                {
                    boost::unique_lock<boost::shared_mutex> lock(rwlock);
                    oldToNewBlockId.erase(oldBlockId);
                    std::vector<CacheItem*>* oldBlock = blockIdToCacheItems[oldBlockId];
                    for (CacheItem* cacheItem: *oldBlock) {
                        delete cacheItem;
                    }
                    delete oldBlock;
                    blockIdToCacheItems.erase(oldBlockId);
                    blockIdToOccupiedSizeMem.erase(oldBlockId);
                    totalDataSizeBytes -= blockIdToSizeMem[oldBlockId];
                    blockIdToSizeMem.erase(oldBlockId);
                    gcBlockIdSet.erase(oldBlockId);

                    long long validSize = 0L;
                    newBlock = blockIdToCacheItems[newBlockId];
                    int offset = 0;
                    for (CacheItem* cacheItem: *newBlock) {
                        cacheItem->offset = offset;
                        offset += cacheItem->size;

                        if (keyToBlockIdMem.find(cacheItem->key) != keyToBlockIdMem.end()) {
                            if (cacheItem->state != ObjectState::DELETED) {
                                keyToBlockIdMem[cacheItem->key] = newBlockId;
                                if (cacheItem->state == ObjectState::VALID) {
                                    validSize += cacheItem->size;
                                }
                            }
                        } else {
                            cacheItem->state = ObjectState::DELETED;
                        }
                    }
                    blockIdToOccupiedSizeMem[newBlockId] = validSize;
                }

                oscClient->DeleteData(oscBucketName, oldBlockId);
            }));

            if (threads.size() >= nThreads) {
                for (auto& thread : threads) {
                    thread.join();
                }
                threads.clear();
            }
        }
        if (threads.size() > 0) {
            for (auto& thread : threads) {
                thread.join();
            }
        }
        threads.clear();
    } else {
        throw new std::runtime_error("DB mode is not supported yet");        
    }
}

std::string MeringueServiceImpl::getNextBlockId() {
    return "M-" + std::to_string(meringueBlockId++);
}


void serve() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("[::]:50052", grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);

    MeringueServiceImpl service;
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    logger->log_message(NAME, "Start listening on [::]:50052");
    server->Wait();
}

int main() {
    logger = new Logger(NAME);
    logger->log_message(NAME, "Start running gRPC server of meringue");
    serve();
    return 0;
}
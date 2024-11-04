#include "packing_block_manager.hpp"

#include <mutex>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "utils.hpp"
#include "meringue.grpc.pb.h"
#include "cloudapis.hpp"

PackingBlock::PackingBlock(int id, long long maxSize, int maxObjectCount) 
            : id(id), maxSize(maxSize), maxObjectCount(maxObjectCount), currentSize(0L), currentObjectCount(0) {};

void PackingBlock::addData(std::string key, std::string data) {
    blockMap[key] = data;
    currentSize += data.size();
    currentObjectCount++;
}

bool PackingBlock::getIfDataExists(std::string key, std::string& value) {
    if (blockMap.find(key) != blockMap.end()) {
        value = blockMap[key];
        return true;
    }
    return false;
}

bool PackingBlock::isFull() {
    return currentSize >= maxSize || currentObjectCount >= maxObjectCount;
}


PackingBlockManager::PackingBlockManager(std::string name, long long maxBlockSize, int maxBlockObjectCount, CloudClient* oscClient, std::string bucketName, std::string _meringueAddress) 
                    : name(name), maxBlockSize(maxBlockSize), maxBlockObjectCount(maxBlockObjectCount), oscClient(oscClient), bucketName(bucketName) {
    globalId = 0;
    creatingBlock = new PackingBlock(globalId++, maxBlockSize, maxBlockObjectCount);
    oscPutCnt = 0;

    meringueAddress = _meringueAddress;
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    channel_args.SetMaxSendMessageSize(64 * 1024 * 1024);
    meringueChannel = grpc::CreateCustomChannel(meringueAddress, grpc::InsecureChannelCredentials(), channel_args);
    meringueStub = meringue::MeringueService::NewStub(meringueChannel);

    logger = new Logger(name);
}

void PackingBlockManager::addData(std::string key, std::string data) {
    int id = -1;
    {
        boost::unique_lock<boost::shared_mutex> lock(c_rwlock);
        creatingBlock->addData(key, data);
        if (creatingBlock->isFull()) {
            id = creatingBlock->getId();
            {
                boost::unique_lock<boost::shared_mutex> lock(d_rwlock);
                lockedBlocks[id] = creatingBlock;
            }
            creatingBlock = new PackingBlock(globalId++, maxBlockSize, maxBlockObjectCount);
        }
    }

    if (id != -1) {
        std::thread t(&PackingBlockManager::sendLockedBlockToOSC, this, id);
        t.detach();
    }
}

void PackingBlockManager::flushBlock() {
    int id;
    {
        boost::unique_lock<boost::shared_mutex> lock(c_rwlock);
        id = creatingBlock->getId();
        {
            boost::unique_lock<boost::shared_mutex> lock(d_rwlock);
            lockedBlocks[id] = creatingBlock;
        }
        creatingBlock = new PackingBlock(globalId++, maxBlockSize, maxBlockObjectCount);
    }
    sendLockedBlockToOSC(id);
}

void PackingBlockManager::deleteData(std::string key) {
    {
        boost::unique_lock<boost::shared_mutex> lock(c_rwlock);
        creatingBlock->blockMap.erase(key);
    }

    {
        boost::unique_lock<boost::shared_mutex> lock(d_rwlock);
        for (auto it = lockedBlocks.begin(); it != lockedBlocks.end(); it++) {
            it->second->blockMap.erase(key);
        }
    }
}

void PackingBlockManager::sendLockedBlockToOSC(int id) {
    PackingBlock* block;
    std::string buffer = "";
    std::unordered_map<std::string, std::pair<int, long long>> blockInfo;
    {
        boost::shared_lock<boost::shared_mutex> lock(d_rwlock);
        block = lockedBlocks[id];

        int offset = 0;
        for (auto it = block->blockMap.begin(); it != block->blockMap.end(); it++) {
            blockInfo[it->first] = std::make_pair(offset, it->second.size());
            buffer += it->second;
            offset += it->second.size();
        }
    }

    std::string blockName = getBlockName(block->getId());
    oscClient->PutData(bucketName, blockName, buffer);
    oscPutCnt++;
    std::string blockMapStr = "";
    
    {
        boost::shared_lock<boost::shared_mutex> lock(d_rwlock);
        for (auto it = blockInfo.begin(); it != blockInfo.end(); it++) {
            if (block->blockMap.find(it->first) == block->blockMap.end()) {
                continue;
            }

            if (blockMapStr.size() > 0) {
                blockMapStr += ",";
            }
            blockMapStr += it->first + "," + std::to_string(it->second.first) + "," + std::to_string(it->second.second);
        }
    }

    meringue::PutSingleMDRequest request;
    meringue::PutSingleMDResponse response;
    grpc::ClientContext context;
    request.set_key(blockName);
    request.set_size(buffer.size());
    request.set_block_map(blockMapStr);

    grpc::Status status = meringueStub->PutSingleMD(&context, request, &response);
    if (!status.ok()) {
        logger->log_message(name, "Error putting metadata into meringue: " + status.error_message());
    }

    {
        boost::unique_lock<boost::shared_mutex> lock(d_rwlock);
        lockedBlocks.erase(id);
        delete block;
    }
}

bool PackingBlockManager::getIfDataExists(std::string key, std::string& value) {
    {
        boost::shared_lock<boost::shared_mutex> lock(c_rwlock);
        if (creatingBlock->getIfDataExists(key, value)) {
            return true;
        }
    }

    {
        boost::shared_lock<boost::shared_mutex> lock(d_rwlock);
        for (auto it = lockedBlocks.begin(); it != lockedBlocks.end(); it++) {
            if (it->second->getIfDataExists(key, value)) {
                return true;
            }
        }
    }
    return false;
}

std::string PackingBlockManager::getBlockName(int id) {
    return name + "-" + std::to_string(id);
}

int PackingBlockManager::getOSCPutCnt() {
    return oscPutCnt;
}

void PackingBlockManager::resetOSCPutCnt() {
    oscPutCnt = 0;
}


#ifndef PACKINGBLOCKMANAGER_HPP
#define PACKINGBLOCKMANAGER_HPP

#include <mutex>
#include <memory>
#include <random>
#include <unordered_map>
#include <string>
#include <atomic>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "utils.hpp"
#include "cloudapis.hpp"
#include "meringue.grpc.pb.h"

class PackingBlock {
public:
    PackingBlock(int id, long long maxSize, int maxObjectCount);
    void addData(std::string key, std::string data);
    bool getIfDataExists(std::string key, std::string& value);
    bool isFull();
    int getId() { return id; }
    
    std::unordered_map<std::string, std::string> blockMap;

private:
    int id;
    long long maxSize;
    int maxObjectCount;
    long long currentSize;
    int currentObjectCount;
};


class PackingBlockManager {
public:
    PackingBlockManager(std::string name, long long maxBlockSize, int maxBlockObjectCount, CloudClient* oscClient, std::string bucketName, std::string meringueAddress);
    void addData(std::string key, std::string data);
    void flushBlock();
    void deleteData(std::string key);
    void sendLockedBlockToOSC(int id);
    bool getIfDataExists(std::string key, std::string& value);
    std::string getBlockName(int id);
    int getOSCPutCnt();
    void resetOSCPutCnt();

private:
    std::string name;
    PackingBlock *creatingBlock;
    std::unordered_map<int, PackingBlock*> lockedBlocks;

    boost::shared_mutex c_rwlock;
    boost::shared_mutex d_rwlock;
    long long maxBlockSize;
    int maxBlockObjectCount;
    CloudClient* oscClient;
    std::string bucketName;
    std::atomic<int> oscPutCnt;

    std::string meringueAddress;
    std::shared_ptr<grpc::Channel> meringueChannel;
    std::unique_ptr<meringue::MeringueService::Stub> meringueStub;
    int globalId = 0;

    Logger* logger;
};

#endif // PACKINGBLOCKMANAGER_HPP
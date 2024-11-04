#include <iostream>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include "utils.hpp"
#include "configs.hpp"
#include "cache_engine.grpc.pb.h"
#include "mbakery.grpc.pb.h"
#include "macaron_client.hpp"

MacaronClient::MacaronClient() {
    std::string filename = "macaron_client_debug.log";
    if (!std::filesystem::exists(MACARON_CLIENT_DEBUG_FILE_PATH)) {
        std::filesystem::create_directories(MACARON_CLIENT_DEBUG_FILE_PATH);
    }
    debugFilePath = MACARON_CLIENT_DEBUG_FILE_PATH + "/" + filename;
    if (!std::filesystem::exists(debugFilePath)) {
        std::ofstream debugFile(debugFilePath, std::ofstream::out);
        debugFile << "MacaronClient is initialized" << std::endl;
        debugFile.close();
    }
    latencyLoggingEnabled = false;
    latencyLogs = new std::vector<std::pair<long long, int>>();
    loggingCount = 0;
}

void MacaronClient::connect(const std::string& mbakeryAddress) {
    std::cout << "Connecting to MBakery server: " << mbakeryAddress << std::endl;
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    channel_args.SetMaxSendMessageSize(64 * 1024 * 1024);
    mbakeryChannel = grpc::CreateCustomChannel(mbakeryAddress, grpc::InsecureChannelCredentials(), channel_args);
    mbakeryStub = mbakery::MBakeryService::NewStub(mbakeryChannel);

    mbakery::ConnectRequest request;
    mbakery::ConnectResponse response;
    grpc::ClientContext context;

    std::cout << "Send Connect request to MBakery server" << std::endl;
    grpc::Status status = mbakeryStub->Connect(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to connect to MBakery server");
    }

    std::cout << "Start connecting to Cache Engines" << std::endl;
    cacheEngineAddresses = new std::vector<std::string>();
    cacheEngineAddressesString = response.message();
    splitString(cacheEngineAddressesString, *cacheEngineAddresses);

    nodeLocator = new ConsistentHashRing();
    for (auto address : *cacheEngineAddresses) {
        nodeLocator->addNode(address);
    }

    cacheEngineChannels = new std::map<std::string, std::shared_ptr<grpc::Channel>>();
    cacheEngineStubs = new std::map<std::string, std::unique_ptr<cacheengine::CacheEngineService::Stub>>();
    for (auto address : *cacheEngineAddresses) {
        std::cout << "Connecting to Cache Engine: " << address << std::endl;
        grpc::ChannelArguments ceChannelArgs;
        ceChannelArgs.SetMaxReceiveMessageSize(64 * 1024 * 1024);
        ceChannelArgs.SetMaxSendMessageSize(64 * 1024 * 1024);
        auto cacheEngineChannel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), ceChannelArgs);
        auto cacheEngineStub = cacheengine::CacheEngineService::NewStub(cacheEngineChannel);
        cacheEngineChannels->insert(std::make_pair(address, cacheEngineChannel));
        cacheEngineStubs->insert(std::make_pair(address, std::move(cacheEngineStub)));
    }

    checkCacheEngineClusterThread = std::thread(&MacaronClient::checkCacheEngineCluster, this);
    checkCacheEngineClusterThread.detach();
}

void MacaronClient::checkCacheEngineCluster() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        mbakery::ConnectRequest request;
        mbakery::ConnectResponse response;
        grpc::ClientContext context;

        std::cout << "Check MBakery for CE cluster" << std::endl;
        grpc::Status status = mbakeryStub->Connect(&context, request, &response);
        if (!status.ok()) {
            std::cout << "Failed to connect to MBakery server" << std::endl;
            continue;
        }

        std::string newClusterString = response.message();
        if (newClusterString == cacheEngineAddressesString) {
            std::cout << "No change in CE cluster" << std::endl;
            continue;
        }

        std::cout << "The CE cluster has been changed. Update accordingly" << std::endl;
        std::vector<std::string> *newAddresses = new std::vector<std::string>();
        splitString(newClusterString, *newAddresses);
        std::ofstream debugFile(debugFilePath, std::ofstream::out | std::ofstream::app);
        for (auto address : *newAddresses) {
            debugFile << "New Cache Engine: " << address << std::endl;
        }
        debugFile.close();

        ConsistentHashRing *newNodeLocator = new ConsistentHashRing();
        for (auto address : *newAddresses) {
            newNodeLocator->addNode(address);
        }

        std::map<std::string, std::shared_ptr<grpc::Channel>> *newCacheEngineChannels = new std::map<std::string, std::shared_ptr<grpc::Channel>>();
        std::map<std::string, std::unique_ptr<cacheengine::CacheEngineService::Stub>> *newCacheEngineStubs = new std::map<std::string, std::unique_ptr<cacheengine::CacheEngineService::Stub>>();
        for (auto address : *newAddresses) {
            std::cout << "Connecting to Cache Engine: " << address << std::endl;
            grpc::ChannelArguments ceChannelArgs;
            ceChannelArgs.SetMaxReceiveMessageSize(64 * 1024 * 1024);
            ceChannelArgs.SetMaxSendMessageSize(64 * 1024 * 1024);
            auto cacheEngineChannel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), ceChannelArgs);
            auto cacheEngineStub = cacheengine::CacheEngineService::NewStub(cacheEngineChannel);
            newCacheEngineChannels->insert(std::make_pair(address, cacheEngineChannel));
            newCacheEngineStubs->insert(std::make_pair(address, std::move(cacheEngineStub)));
        }
       
        auto oldCacheEngineAddresses = cacheEngineAddresses;
        auto oldNodeLocator = nodeLocator;
        auto oldCacheEngineChannels = cacheEngineChannels;
        auto oldCacheEngineStubs = cacheEngineStubs;

        {
            std::cout << "Update the CE cluster" << std::endl;
            boost::unique_lock<boost::shared_mutex> lock(rwlock);
            cacheEngineAddressesString = newClusterString;
            cacheEngineAddresses = newAddresses;
            nodeLocator = newNodeLocator;
            cacheEngineChannels = newCacheEngineChannels;
            cacheEngineStubs = newCacheEngineStubs;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));
        delete oldCacheEngineAddresses;
        delete oldNodeLocator;
        for (auto it = oldCacheEngineChannels->begin(); it != oldCacheEngineChannels->end(); it++) {
            it->second.reset();
        }
        delete oldCacheEngineChannels;
        for (auto it = oldCacheEngineStubs->begin(); it != oldCacheEngineStubs->end(); it++) {
            it->second.reset();
        }
        delete oldCacheEngineStubs;
    }
}

void MacaronClient::TriggerOptimization() {
    mbakery::OptTriggerRequest request;
    mbakery::OptTriggerResponse response;
    grpc::ClientContext context;
    grpc::Status status = mbakeryStub->OptTrigger(&context, request, &response);
    if (!status.ok() || !response.success()) {
        throw std::runtime_error("Failed to trigger optimization");
    }
}

void MacaronClient::StopOptimization() {
    mbakery::OptStopRequest request;
    mbakery::OptStopResponse response;
    grpc::ClientContext context;
    grpc::Status status = mbakeryStub->OptStop(&context, request, &response);
    if (!status.ok() || !response.success()) {
        throw std::runtime_error("Failed to stop optimization");
    }
}

void MacaronClient::addLatencyLog(std::string key, long long latency, int src) {
    {
        boost::unique_lock<boost::shared_mutex> lock(loglock);
        latencyLogs->push_back(std::make_pair(latency, src));
    }
}

void MacaronClient::writeLatencyLogs(std::string latencyLogFilePath) {
    std::filesystem::path path = latencyLogFilePath;
    if (!std::filesystem::exists(path.parent_path())) {
        std::filesystem::create_directories(path.parent_path());
    }

    std::vector<std::pair<long long, int>>* oldLogs;
    {
        boost::unique_lock<boost::shared_mutex> lock(loglock);
        oldLogs = latencyLogs;
        latencyLogs = new std::vector<std::pair<long long, int>>();
    }

    std::ofstream latencyLogFile(latencyLogFilePath, std::ofstream::out | std::ofstream::binary);
    for (auto log : *oldLogs) {
        latencyLogFile.write(reinterpret_cast<const char*>(&log.first), sizeof(log.first));
        latencyLogFile.write(reinterpret_cast<const char*>(&log.second), sizeof(log.second));
    }
    latencyLogFile.close();
    oldLogs->clear();
    delete oldLogs;
    loggingCount--;
}

void MacaronClient::startLoggingLatency(int seconds) {
    latencyLoggingThread = std::thread([this, seconds]() {
        if (!std::filesystem::exists(MACARON_CLIENT_LATENCY_LOG_FILE_PATH)) {
            std::filesystem::create_directories(MACARON_CLIENT_LATENCY_LOG_FILE_PATH);
        }
        while (latencyLoggingEnabled) {
            std::this_thread::sleep_for(std::chrono::seconds(seconds));
            long long currentTimestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            std::string latencyLogFilePath = MACARON_CLIENT_LATENCY_LOG_FILE_PATH + "/" + std::to_string(currentTimestamp) + ".bin";
            loggingCount++;
            std::thread writeLatencyLogsThread(&MacaronClient::writeLatencyLogs, this, latencyLogFilePath);
            writeLatencyLogsThread.detach();
        }
    });
    latencyLoggingThread.detach();
}

void MacaronClient::waitLatencyLogging() {
    latencyLoggingEnabled = false;
    while (loggingCount > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void MacaronClient::enableLatencyLogging() {
    latencyLoggingEnabled = true;
    latencyLogs->clear();
}

void MacaronClient::disableLatencyLogging() {
    latencyLoggingEnabled = false;
    latencyLogs->clear();
}

void MacaronClient::clearDRAMCache() {
    grpc::Status status;
    bool first = true;
    do {
        if (!first)
            std::this_thread::sleep_for(std::chrono::seconds(5));
        first = false;
        cacheengine::ClearDRAMCacheRequest request;
        cacheengine::ClearDRAMCacheResponse response;
        grpc::ClientContext context;
        status = cacheEngineStubs->at(cacheEngineAddresses->at(0))->ClearDRAMCache(&context, request, &response);
    } while (!status.ok());
}

void MacaronClient::flushCacheEngineLatencyLog(std::string latencyLogFilePath) {
    cacheengine::FlushLatencyLogRequest request;
    cacheengine::FlushLatencyLogResponse response;
    grpc::ClientContext context;
    request.set_log_file_path(latencyLogFilePath);
    
    grpc::Status status = cacheEngineStubs->at(cacheEngineAddresses->at(0))->FlushLatencyLog(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to flush latency log of cache engine");
    }
}

void MacaronClient::Put(const std::string& key, const std::string& data) {
    std::string address;
    cacheengine::CacheEngineService::Stub *stub;
    {
        boost::shared_lock<boost::shared_mutex> lock(rwlock);
        address = nodeLocator->getNode(key);
        stub = cacheEngineStubs->at(address).get();
    }

    cacheengine::PutRequest request;
    cacheengine::PutResponse response;
    grpc::ClientContext context;
    request.set_key(key);
    request.set_data(data);
    grpc::Status status = stub->Put(&context, request, &response);
    if (!status.ok()) {
        std::ofstream debugFile(debugFilePath, std::ofstream::out | std::ofstream::app);
        debugFile << "Failed to put data from Macaron cache. Key: " << key << std::endl;
        debugFile << "Error: " << status.error_message() << std::endl;
        debugFile.close();
    }
}

bool MacaronClient::Get(const std::string& key, std::string& response_data) {
    std::string address;
    cacheengine::CacheEngineService::Stub *stub;
    {
        boost::shared_lock<boost::shared_mutex> lock(rwlock);
        address = nodeLocator->getNode(key);
        stub = cacheEngineStubs->at(address).get();
    }

    cacheengine::GetRequest request;
    cacheengine::GetResponse response;
    grpc::ClientContext context;
    request.set_key(key);
    
    auto start = std::chrono::high_resolution_clock::now();
    grpc::Status status = stub->Get(&context, request, &response);
    long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count();
    if (latencyLoggingEnabled) {
        addLatencyLog(key, microseconds, response.src());
    }

    if (!status.ok()) {
        std::ofstream debugFile(debugFilePath, std::ofstream::out | std::ofstream::app);
        debugFile << "Failed to get data from Macaron cache. Key: " << key << std::endl;
        debugFile << "Error: " << status.error_message() << std::endl;
        debugFile.close();
        return false;
    }

    if (response.exists()) {
        response_data = response.data();
        return true;
    } else {
        return false;
    }
}

void MacaronClient::Delete(const std::string& key) {
    std::string address;
    cacheengine::CacheEngineService::Stub *stub;
    {
        boost::shared_lock<boost::shared_mutex> lock(rwlock);
        address = nodeLocator->getNode(key);
        stub = cacheEngineStubs->at(address).get();
    }

    cacheengine::DeleteRequest request;
    cacheengine::DeleteResponse response;
    grpc::ClientContext context;
    request.set_key(key);
    grpc::Status status = stub->Delete(&context, request, &response);
    if (!status.ok()) {
        std::ofstream debugFile(debugFilePath, std::ofstream::out | std::ofstream::app);
        debugFile << "Failed to delete data from Macaron cache. Key: " << key << std::endl;
        debugFile << "Error: " << status.error_message() << std::endl;
        debugFile.close();
    }
}



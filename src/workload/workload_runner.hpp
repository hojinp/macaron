#ifndef WORKLOADRUNNER_HPP
#define WORKLOADRUNNER_HPP

#include <condition_variable> 
#include <queue>
#include <iostream>
#include <thread>
#include <mutex>
#include <fstream>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>

#include "macaron_client.hpp"
#include "cache_engine_logger.hpp"

class RequestProcessor {
public:
    RequestProcessor(std::string mbakery_address, size_t maxSize, size_t numThreads, bool logLatency);

    void addRequest(LogEntry* entry);

    void processRequests();

    void stopProcessing();

private:
    void worker();

    std::queue<LogEntry*> requests;
    std::mutex mtx;
    std::condition_variable cv;
    size_t maxQueueSize;
    size_t numThreads;
    std::vector<std::thread*> workerThreads;
    bool allRequestsSent = false;

    MacaronClient* client;
    std::chrono::time_point<std::chrono::system_clock> startTimestamp;
};

#endif // WORKLOADRUNNER_HPP
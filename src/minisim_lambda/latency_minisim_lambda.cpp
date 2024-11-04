#include <iostream>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>
#include <thread>
#include <aws/lambda-runtime/runtime.h>
#include <jsoncpp/json/json.h>
#include <crypto++/sha.h>

#include "rocksdbapis.hpp"
#include "latency_lru_cache_lambda.hpp"
#include "cache_engine_logger.hpp"
#include "latency_minisim_lambda.hpp"
#include "latency_generator.hpp"


static aws::lambda_runtime::invocation_response latency_minisim_handler(aws::lambda_runtime::invocation_request const& request) {
    std::cout << "Triggered latency_minisim_handler" << std::endl;

    Json::Value requestPayloads;
    Json::Reader jsonReader;
    jsonReader.parse(request.payload, requestPayloads);

    long long minute = requestPayloads["Minute"].asInt64();
    bool contd = requestPayloads["Contd"].asBool();
    std::string mnt_path = requestPayloads["MountPath"].asString();
    std::string expName = requestPayloads["ExpName"].asString();
    long long dramSizeMB = requestPayloads["DRAMSizeMB"].asInt64();
    long long oscSizeByte = requestPayloads["OSCSizeByte"].asInt64();
    int minisimHashSize = requestPayloads["MinisimHashSize"].asInt();
    double samplingRatio = requestPayloads["SamplingRatio"].asDouble();
    std::string latencyDirPath = requestPayloads["LatencyDirPath"].asString();
    std::string logDirPath = requestPayloads["LogDirPath"].asString();
    std::string remoteRegion = requestPayloads["RemoteRegion"].asString();
    int logFileCount = requestPayloads["LogFileCount"].asInt();
    std::cout << "Minute: " << minute << ", Contd: " << contd << ", ExpName: " << expName << ", DRAMSizeMB: " << dramSizeMB << ", OSCSizeByte: " << oscSizeByte << ", SamplingRatio: " << samplingRatio << std::endl;
    std::cout << "LatencyDirPath: " << latencyDirPath << ", LogDirPath: " << logDirPath << ", RemoteRegion: " << remoteRegion << ", LogFileCount: " << logFileCount << std::endl;

    long long MB_TO_BYTE = 1024LL * 1024LL;
    long long sampledDRAMSizeByte = (long long) (((long long) dramSizeMB) * MB_TO_BYTE * samplingRatio);
    long long sampledOSCSizeByte = (long long) (oscSizeByte * samplingRatio);
    int sampledHashSpace = (int) (minisimHashSize * samplingRatio);

    std::string minisimPath = mnt_path + "/" + expName + "/latency_minisim/" + std::to_string(dramSizeMB) + "MB";
    std::string dramCacheMinisimPath = minisimPath + "/dram";
    std::string oscCacheMinisimPath = minisimPath + "/osc";
    rocksdb::DB* dramCacheDB = rd_create_db(contd, dramCacheMinisimPath, "db");
    rocksdb::DB* oscCacheDB = rd_create_db(contd, oscCacheMinisimPath, "db");
    std::cout << "Opened RocksDB" << std::endl;

    auto start_time = std::chrono::high_resolution_clock::now();
    std::mutex logMutex;
    std::vector<LogEntry*> logEntries;
    int Nthreads = 32;
    std::vector<std::thread> threads;
    int hashSize = minisimHashSize;
    int totalRequestCount = 0;
    while (true) {
        // check until the number of log files in logDirPath is equal to the logFileCount
        int count = 0;
        for (const auto& entry: std::filesystem::directory_iterator(logDirPath)) {
            if (entry.is_regular_file()) {
                count++;
            }
        }
        if (count == logFileCount) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    for (const auto& entry: std::filesystem::directory_iterator(logDirPath)) {
        threads.emplace_back([&logEntries, &logMutex, &hashSize, &sampledHashSpace, &totalRequestCount](const std::filesystem::directory_entry& entry) {
            if (entry.is_regular_file()) {
                std::vector<LogEntry*> localLogEntries;
                int requestCount = readCacheEngineLogFileWithSamplingByChunk(entry.path(), localLogEntries, hashSize, sampledHashSpace);

                std::lock_guard<std::mutex> lock(logMutex);
                logEntries.insert(logEntries.end(), localLogEntries.begin(), localLogEntries.end());
                totalRequestCount += requestCount;
            }
        }, entry);

        if (threads.size() >= Nthreads) {
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
    int read_count = logEntries.size();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Read " << read_count << " log entries in " << read_time << " ms" << std::endl;
    
    start_time = std::chrono::high_resolution_clock::now();
    std::sort(logEntries.begin(), logEntries.end(), [](LogEntry* a, LogEntry* b) {
        return a->timestamp < b->timestamp;
    });
    end_time = std::chrono::high_resolution_clock::now();
    auto sort_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Sorted " << read_count << " log entries in " << sort_time << " ms" << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    std::string dramStateFilePath = dramCacheMinisimPath + "/state.csv";
    std::string oscStateFilePath = oscCacheMinisimPath + "/state.csv";
    LatencyLRUCacheLambda *dramCache = new LatencyLRUCacheLambda(sampledDRAMSizeByte, dramCacheDB);
    LatencyLRUCacheLambda *oscCache = new LatencyLRUCacheLambda(sampledOSCSizeByte, oscCacheDB);
    dramCache->loadState(dramStateFilePath);
    oscCache->loadState(oscStateFilePath);
    oscCache->updateCacheSize(sampledOSCSizeByte);

    LatencyGenerator* latencyGenerator = new LatencyGenerator(latencyDirPath, remoteRegion);

    std::cout << "Start running miniature simulation" << std::endl;
    int sampledGetCount = 0;
    std::vector<long long> latencies_;
    for (LogEntry* entry: logEntries) {
        std::string key = entry->key;
        long long size = entry->size;
        long long timestamp = entry->timestamp * 1000LL; // convert ms to us

        if (entry->opType == CloudOperation::GET) {
            sampledGetCount++;
            
            long long dramLat = latencyGenerator->getAvgDRAMLatency(size);
            long long oscLat = latencyGenerator->getAvgOSCLatency(size);
            long long dlLat = latencyGenerator->getAvgDLLatency(size);
    
            long long dramBirth = dramCache->getBirth(key);
            long long oscBirth = oscCache->getBirth(key);

            if (dramBirth == -1LL) { // miss from DRAM
                if (oscBirth == -1LL) { // miss from OSC
                    latencies_.push_back(dlLat);
                } else {
                    if (oscBirth + dlLat <= timestamp) { // miss from DRAM, hit from OSC (already in OSC)
                        latencies_.push_back(oscLat);
                    } else { // miss from DRAM, waiting for retrieving data from DL
                        latencies_.push_back(dlLat - (timestamp - oscBirth));
                    }
                }
            } else { // hit from DRAM
                if (oscBirth == -1LL) {
                    throw std::runtime_error("Invalid state");
                }
                
                if (oscBirth + dlLat <= timestamp) { // hit from DRAM and OSC (already in OSC)
                    if (dramBirth + oscLat <= timestamp) { // hit from DRAM
                        latencies_.push_back(dramLat);
                    } else { // miss from DRAM and hit from OSC
                        latencies_.push_back(oscLat);
                    }
                } else { // somehow, hit from DRAM, but OSC is still waiting for retrieving data from DL
                    if (dramBirth + dlLat <= timestamp) { // hit from DRAM
                        latencies_.push_back(dramLat);
                    } else { // hit from DRAM, but still waiting for retrieving data from DL
                        latencies_.push_back(dlLat - (timestamp - dramBirth));
                    }
                }
            }

            if (dramBirth == -1LL) {
                dramCache->put(key, size, timestamp);
            } else {
                dramCache->get(key);
            }

            if (oscBirth == -1LL) {
                oscCache->put(key, size, timestamp);
            } else {
                oscCache->get(key);
            }
        } else if (entry->opType == CloudOperation::PUT) {
            dramCache->put(key, size, timestamp);
            oscCache->put(key, size, timestamp);
        } else if (entry->opType == CloudOperation::DELETE) {
            dramCache->del(key);
            oscCache->del(key);
        }
    }
    end_time = std::chrono::high_resolution_clock::now();
    auto run_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Processed " << read_count << " log entries for RUN in " << run_time << " ms" << std::endl;

    std::string stat_dirname = minisimPath + "/stats";
    if (!std::filesystem::exists(stat_dirname)) {
        std::filesystem::create_directories(stat_dirname);
    }

    std::string perf_filename = stat_dirname + "/performance.csv";
    if (!std::filesystem::exists(perf_filename)) {
        std::ofstream perfFile(perf_filename);
        perfFile << "Minute,Method,Count,Time(ms)" << std::endl;
        perfFile.close();
    }
    std::ofstream perfFile(perf_filename, std::ofstream::out | std::ofstream::app);
    perfFile << minute << ",read," << read_count << "," << read_time << std::endl;
    perfFile << minute << ",sort," << read_count << "," << sort_time << std::endl;
    perfFile << minute << ",run," << read_count << "," << run_time << std::endl;
    perfFile.close();

    std::string stat_filename = stat_dirname + "/stats.csv";
    if (!std::filesystem::exists(stat_filename)) {
        std::ofstream statFile(stat_filename);
        statFile << "Minute,CacheSize,AvgLatency" << std::endl;
        statFile.close();
    }
    std::ofstream statFile(stat_filename, std::ofstream::out | std::ofstream::app);
    double avgLatency = 0.;
    for (long long lat : latencies_) {
        avgLatency += (double) lat / (double) latencies_.size();
    }
    statFile << minute << "," << dramSizeMB << "," << (long long) avgLatency << std::endl;
    statFile.close();

    std::string req_filename = stat_dirname + "/reqcount.csv";
    if (!std::filesystem::exists(req_filename)) {
        std::ofstream reqFile(req_filename);
        reqFile << "Minute,SampledGetCount,TotalRequestCount" << std::endl;
        reqFile.close();
    }
    std::ofstream reqFile(req_filename, std::ofstream::out | std::ofstream::app);
    reqFile << minute << "," << sampledGetCount << "," << totalRequestCount << std::endl;
    reqFile.close();

    dramCache->saveState(dramStateFilePath);
    oscCache->saveState(oscStateFilePath);
    
    delete dramCache;
    delete oscCache;

    dramCacheDB->Close();
    oscCacheDB->Close();

    delete dramCacheDB;
    delete oscCacheDB;
    
    delete latencyGenerator;

    return aws::lambda_runtime::invocation_response::success("Done!", "application/json");
}

int main() {
    aws::lambda_runtime::run_handler(latency_minisim_handler);
    return 0;
}
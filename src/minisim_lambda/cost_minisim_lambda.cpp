#include <iostream>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <aws/lambda-runtime/runtime.h>
#include <jsoncpp/json/json.h>

#include "utils.hpp"
#include "configs.hpp"
#include "rocksdbapis.hpp"
#include "cost_lru_cache_lambda.hpp"
#include "cache_engine_logger.hpp"
#include "cost_minisim_lambda.hpp"



static aws::lambda_runtime::invocation_response cost_minisim_handler(aws::lambda_runtime::invocation_request const& request) {
    std::cout << "Triggered cost_minisim_handler" << std::endl;

    Json::Value requestPayloads;
    Json::Reader jsonReader;
    jsonReader.parse(request.payload, requestPayloads);

    long long minute = requestPayloads["Minute"].asInt64();
    bool contd = requestPayloads["Contd"].asBool();
    std::string mnt_path = requestPayloads["MountPath"].asString();
    std::string expName = requestPayloads["ExpName"].asString();
    long long cacheSizeMB = requestPayloads["CacheSizeMB"].asInt64();
    int minisimHashSize = requestPayloads["MinisimHashSize"].asInt();
    double samplingRatio = requestPayloads["SamplingRatio"].asDouble();
    std::string logDirPath = requestPayloads["LogDirPath"].asString();
    std::cout << "Minute: " << minute << ", Contd: " << contd << ", ExpName: " << expName << ", CacheSizeMB: " << cacheSizeMB << ", SamplingRatio: " << samplingRatio << ", LogDirPath: " << logDirPath << std::endl;

    long long MB_TO_BYTE = 1024LL * 1024LL;
    double sampledCacheSizeByte = (long long) (((long long) cacheSizeMB) * MB_TO_BYTE * samplingRatio);
    int sampledHashSpace = (int) (minisimHashSize * samplingRatio);

    std::string minisimPath = mnt_path + "/" + expName + "/cost_minisim/" + std::to_string(cacheSizeMB) + "MB";
    rocksdb::DB* db = rd_create_db(contd, minisimPath, "db");

    auto start_time = std::chrono::high_resolution_clock::now();
    std::mutex logMutex;
    std::vector<LogEntry*> logEntries;
    int Nthreads = 32;
    std::vector<std::thread> threads;
    int hashSize = minisimHashSize;
    for (const auto& entry: std::filesystem::directory_iterator(logDirPath)) {
        threads.emplace_back([&logEntries, &logMutex, &hashSize, &sampledHashSpace](const std::filesystem::directory_entry& entry) {
            if (entry.is_regular_file()) {
                std::vector<LogEntry*> localLogEntries;
                readCacheEngineLogFileWithSamplingByChunk(entry.path(), localLogEntries, hashSize, sampledHashSpace);

                std::lock_guard<std::mutex> lock(logMutex);
                logEntries.insert(logEntries.end(), localLogEntries.begin(), localLogEntries.end());
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
    std::string stateFilePath = minisimPath + "/state.csv";
    CostLRUCacheLambda *cache = new CostLRUCacheLambda(sampledCacheSizeByte, db);
    cache->loadState(stateFilePath);

    std::cout << "Start running miniature simulation" << std::endl;
    int sampledGetCount = 0, sampledPutCount = 0;
    for (LogEntry* entry: logEntries) {
        std::string key = entry->key;
        long long size = entry->size;

        if (entry->opType == CloudOperation::GET) {
            sampledGetCount++;
            if (cache->get(key) == 0LL) {
                cache->put(key, size);
                cache->addBytesMiss(size);
            }
        } else if (entry->opType == CloudOperation::PUT) {
            sampledPutCount++;
            cache->put(key, size);
        } else if (entry->opType == CloudOperation::DELETE) {
            cache->del(key);
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
        statFile << "Minute,CacheSize,HitCount,MissCount,BytesMiss" << std::endl;
        statFile.close();
    }
    std::ofstream statFile(stat_filename, std::ofstream::out | std::ofstream::app);
    int hit, miss;
    long long bytesMiss;
    cache->getTmpHitMiss(hit, miss);
    cache->getTmpBytesMiss(bytesMiss);
    bytesMiss /= samplingRatio;
    statFile << minute << "," << cacheSizeMB << "," << hit << "," << miss << "," << bytesMiss << std::endl;

    std::string req_filename = stat_dirname + "/reqcount.csv";
    if (!std::filesystem::exists(req_filename)) {
        std::ofstream reqFile(req_filename);
        reqFile << "Minute,SampledGetCount,SampledPutCount" << std::endl;
        reqFile.close();
    }
    std::ofstream reqFile(req_filename, std::ofstream::out | std::ofstream::app);
    reqFile << minute << "," << sampledGetCount << "," << sampledPutCount << std::endl;
    reqFile.close();

    cache->saveState(stateFilePath);
    db->Close();
    return aws::lambda_runtime::invocation_response::success("Done!", "application/json");
}

int main() {
    aws::lambda_runtime::run_handler(cost_minisim_handler);
    return 0;
}

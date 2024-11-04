#include <sstream>
#include <iomanip>
#include <string>
#include <vector>
#include <thread>
#include <future>
#include <functional>
#include <chrono>
#include <numeric>

#include "configs.hpp"
#include "utils.hpp"
#include "rocksdbapis.hpp"
#include "cache_engine_logger.hpp"
#include "cost_lru_cache.hpp"
#include "cost_minisim.hpp"

CostMiniatureSimulation::CostMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitBM_) {
    samplingRatio = samplingRatio_;
    miniCacheCount = miniCacheCount_;
    cacheSizeUnitMB = cacheSizeUnitBM_;
}

void CostMiniatureSimulation::loadLastLambdaData(std::string baseDirPath, long long minute_) {
    std::vector<int>* tmpHits = new std::vector<int>();
    std::vector<int>* tmpMisses = new std::vector<int>();
    std::vector<long long>* tmpBytesMisses = new std::vector<long long>();

    for (int i = 1; i <= miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * i;
        std::string dirPath = baseDirPath + "/" + std::to_string(cacheSizeMB) + "MB/stats";
        if (i == 1) {
            while (true) {
                std::string reqFilename = dirPath + "/reqcount.csv", line;
                bool exists = readLastLineFromFile(reqFilename, line);
                if (!exists) {
                    std::ofstream ofs("/tmp/debugmacaron.txt", std::ios::out | std::ios::app);
                    ofs << "File does not exist: " << reqFilename << "\n";
                    ofs.close();
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                long long minute;
                int sampledGetCount, sampledPutCount;
                char comma;
                std::istringstream iss(line);
                iss >> minute >> comma >> sampledGetCount >> comma >> sampledPutCount;

                if (minute != minute_) {
                    std::ofstream ofs("/tmp/debugmacaron.txt", std::ios::out | std::ios::app);
                    ofs << "Minute is not matching: " << minute << " != " << minute_ << "\n";
                    ofs << "Line: " << line << "\n";
                    ofs.close();
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                minutes.push_back((int) minute);
                sampledGetCounts.push_back(sampledGetCount);
                sampledPutCounts.push_back(sampledPutCount);
                break;
            }
        }
        
        while (true) {
            std::string statFilename = dirPath + "/stats.csv", line;
            bool exists = readLastLineFromFile(statFilename, line);
            if (!exists) {
                std::ofstream ofs("/tmp/debugmacaron.txt", std::ios::out | std::ios::app);
                ofs << "File does not exist: " << statFilename << "\n";
                ofs.close();
                std::this_thread::sleep_for(std::chrono::seconds(1));
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            long long minute;
            int hit, miss;
            long long tmpCacheSizeMB, bytesMiss;
            char comma;
            std::istringstream iss(line);
            iss >> minute >> comma >> tmpCacheSizeMB >> comma >> hit >> comma >> miss >> comma >> bytesMiss;
            if (minute != minute_) {
                std::ofstream ofs("/tmp/debugmacaron.txt", std::ios::out | std::ios::app);
                ofs << "Minute is not matching: " << minute << " != " << minute_ << "\n";
                ofs << "Line: " << line << "\n";
                ofs.close();
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            
            tmpHits->push_back(hit);
            tmpMisses->push_back(miss);
            tmpBytesMisses->push_back(bytesMiss);
            break;
        }
    }
    hits.push_back(tmpHits);
    misses.push_back(tmpMisses);
    bytesMisses.push_back(tmpBytesMisses);
}

CostMiniatureSimulation::CostMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_, std::string dbBaseDirPath_) {
    samplingRatio = samplingRatio_;
    miniCacheCount = miniCacheCount_;
    cacheSizeUnitMB = cacheSizeUnitMB_;

    rocksdb::Options options;
    options.create_if_missing = true;
    db = rd_create_db(false, dbBaseDirPath_, "costminisim.db");

    for (int i = 0; i < miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        long long sampledCacheSizeByte = (long long) (((long long) cacheSizeMB) * MB_TO_BYTE * samplingRatio);
        caches[cacheSizeMB] = new CostLRUCache(std::to_string(cacheSizeMB), sampledCacheSizeByte, db);
    }

    statFilePath = dbBaseDirPath_ + "/costminisim_stat.csv";
    reqCountFilePath = dbBaseDirPath_ + "/costminisim_reqcount.csv";
    writeLineToFile(statFilePath, "Minute,CacheSize,MissRatio,BytesMiss", false);
    writeLineToFile(reqCountFilePath, "Minute,ReqCount,SampledReqCount", false);

    debugFilePath = dbBaseDirPath_ + "/costminisim_debug.csv";
    writeLineToFile(debugFilePath, "Minute,Method,Count,ElapsedTime", false);
}

void CostMiniatureSimulation::run(long long minute, std::vector<LogEntry*>& logEntries) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::future<std::pair<int, int>>> futures;
    for (int i = 0; i < miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        CostLRUCache* cache = caches[cacheSizeMB];
        futures.push_back(std::async(std::launch::async, [cache, &logEntries]() {
            int sampledGetCount = 0, sampledPutCount = 0;
            for (LogEntry* entry : logEntries) {
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

            return std::make_pair(sampledGetCount, sampledPutCount);
        }));
    }

    std::pair<int, int> sampledReqCountPair;
    for (int i = 0; i < miniCacheCount; i++) {
        sampledReqCountPair = futures[i].get();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    writeLineToFile(debugFilePath, std::to_string(minute) + ",run," + std::to_string(logEntries.size()) + "," + std::to_string(elapsed_time), true);

    std::vector<int>* tmpHits = new std::vector<int>();
    std::vector<int>* tmpMisses = new std::vector<int>();
    std::vector<long long>* tmpBytesMisses = new std::vector<long long>();
    for (int i = 0; i < miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        CostLRUCache* cache = caches[cacheSizeMB];
        int hit, miss;
        cache->getTmpHitMiss(hit, miss);
        long long bytesMiss;
        cache->getTmpBytesMiss(bytesMiss);
        bytesMiss /= samplingRatio;
        cache->resetTmps();

        tmpHits->push_back(hit);
        tmpMisses->push_back(miss);
        tmpBytesMisses->push_back(bytesMiss);

        double missRatio = hit + miss > 0 ? (double) miss / (hit + miss) : -1.0;
        std::ostringstream stream;
        stream << std::fixed << std::setprecision(6) << missRatio;
        std::string missRatioStr = stream.str();
        writeLineToFile(statFilePath, std::to_string(minute) + "," + std::to_string(cacheSizeMB) + "," + missRatioStr + "," + std::to_string(bytesMiss), true);
    }
    minutes.push_back(minute);
    hits.push_back(tmpHits);
    misses.push_back(tmpMisses);
    bytesMisses.push_back(tmpBytesMisses);
    sampledGetCounts.push_back(sampledReqCountPair.first);
    sampledPutCounts.push_back(sampledReqCountPair.second);

    writeLineToFile(reqCountFilePath, std::to_string(minute) + "," + std::to_string(logEntries.size()) + "," + std::to_string(sampledReqCountPair.first), true);
}

void CostMiniatureSimulation::getCacheSizeMBs(int cacheSizeMBs[]) {
    for (int i = 0; i < miniCacheCount; i++) {
        cacheSizeMBs[i] = cacheSizeUnitMB * (i + 1);
    }
}

void CostMiniatureSimulation::getMRC(double mrcs[]) {
    int sumSampledGetCount = std::accumulate(sampledGetCounts.begin(), sampledGetCounts.end(), 0);
    int monitoredTime = minutes[minutes.size() - 1];
    if (sumSampledGetCount == 0 || monitoredTime < WARMUP_TIME_MINUTE) {
        std::fill(mrcs, mrcs + miniCacheCount, -1.0);
        return;
    } else {
        std::fill(mrcs, mrcs + miniCacheCount, 0.0);
    }

    double coefficients[sampledGetCounts.size()];

    for (int i = 0; i < sampledGetCounts.size(); i++) {
        coefficients[i] = static_cast<double>(sampledGetCounts[i]) / sumSampledGetCount;
    }

    for (int i = 0; i < sampledGetCounts.size(); i++) {
        for (int j = 0; j < miniCacheCount; j++) {
            if (sampledGetCounts[i] != 0) {
                mrcs[j] += coefficients[i] * static_cast<double>(misses[i]->at(j)) / sampledGetCounts[i];
            }
        }
    }
}

void CostMiniatureSimulation::getBMC(long long bytesMisses_[]) {
    int monitoredTime = minutes[minutes.size() - 1];
    if (monitoredTime < WARMUP_TIME_MINUTE) {
        return;
    }
    std::fill(bytesMisses_, bytesMisses_ + miniCacheCount, 0LL);

    double coefficients[sampledGetCounts.size()];

    // TODO: Add decay mechanism
    for (int i = 0; i < sampledGetCounts.size(); i++) {
        coefficients[i] = 1. / sampledGetCounts.size();
    }

    for (int i = 0; i < sampledGetCounts.size(); i++) {
        for (int j = 0; j < miniCacheCount; j++) {
            bytesMisses_[j] += coefficients[i] * (bytesMisses[i]->at(j));
        }
    }
}

void CostMiniatureSimulation::getReqCounts(int& numGets, int& numPuts) {
    int monitoredTime = minutes[minutes.size() - 1];
    if (monitoredTime < WARMUP_TIME_MINUTE) {
        return;
    }

    double coefficients[sampledGetCounts.size()];

    for (int i = 0; i < sampledGetCounts.size(); i++) {
        coefficients[i] = 1. / sampledGetCounts.size();
    }

    for (int i = 0; i < sampledGetCounts.size(); i++) {
        numGets += coefficients[i] / samplingRatio * sampledGetCounts[i];
        numPuts += coefficients[i] / samplingRatio * sampledPutCounts[i];
    }
}


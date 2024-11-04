#include <vector>
#include <future>
#include <thread>
#include <chrono>
#include <functional>
#include <sstream>

#include "latency_minisim.hpp"
#include "rocksdbapis.hpp"
#include "configs.hpp"
#include "utils.hpp"

LatencyMiniatureSimulation::LatencyMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_) {
    samplingRatio = samplingRatio_;
    miniCacheCount = miniCacheCount_;
    cacheSizeUnitMB = cacheSizeUnitMB_;
}

void LatencyMiniatureSimulation::loadLastLambdaData(std::string baseDirPath, long long minute_) {
    std::vector<long long>* tmpLatencies = new std::vector<long long>(); 
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
                int sampledGetCount, totalRequestCount;
                char comma;
                std::istringstream iss(line);
                iss >> minute >> comma >> sampledGetCount >> comma >> totalRequestCount;

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
                totalRequestCounts.push_back(totalRequestCount);
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
                continue;
            }

            long long minute;
            int tmpCacheSizeMB;
            long long avgLatency;
            char comma;
            std::istringstream iss(line);
            iss >> minute >> comma >> tmpCacheSizeMB >> comma >> avgLatency;

            if (minute != minute_) {
                std::ofstream ofs("/tmp/debugmacaron.txt", std::ios::out | std::ios::app);
                ofs << "Minute is not matching: " << minute << " != " << minute_ << "\n";
                ofs << "Line: " << line << "\n";
                ofs.close();
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            tmpLatencies->push_back(avgLatency);
            break;
        }
    }
    latencies.push_back(tmpLatencies);
}

LatencyMiniatureSimulation::LatencyMiniatureSimulation(double samplingRatio_, int miniCacheCount_, int cacheSizeUnitMB_, long long oscSizeByte, std::string dbBaseDirPath_) {
    samplingRatio = samplingRatio_;
    miniCacheCount = miniCacheCount_;
    cacheSizeUnitMB = cacheSizeUnitMB_;

    rocksdb::Options options;
    options.create_if_missing = true;
    db = rd_create_db(false, dbBaseDirPath_, "latencyminisim.db");

    for (int i = 0; i < miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        long long sampledCacheSizeByte = (long long) (((long long) cacheSizeMB) * MB_TO_BYTE * samplingRatio);
        dramCaches[cacheSizeMB] = new LatencyLRUCache("d" + std::to_string(cacheSizeMB), sampledCacheSizeByte, db);
        oscCaches[cacheSizeMB] = new LatencyLRUCache("o" + std::to_string(cacheSizeMB), oscSizeByte * samplingRatio, db);
    }

    latencyGenerator = new LatencyGenerator(MACARON_DATA_DIR_PATH, REMOTE_REGION);

    statFilePath = dbBaseDirPath_ + "/latencyminisim_stat.csv";
    reqCountFilePath = dbBaseDirPath_ + "/latencyminisim_reqcount.csv";
    writeLineToFile(statFilePath, "Minute,CacheSize,Latency(us)", false);
    writeLineToFile(reqCountFilePath, "Minute,ReqCount,SampledReqCount", false);
}

void LatencyMiniatureSimulation::run(long long minute, std::vector<LogEntry*>& logEntries) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::future<std::pair<int, long long>>> futures;
    for (int i = 0; i < miniCacheCount; i++) {
        const int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        futures.push_back(std::async(std::launch::async, [this, cacheSizeMB, &logEntries]() {
            LatencyLRUCache* dramCache = dramCaches[cacheSizeMB];
            LatencyLRUCache* oscCache = oscCaches[cacheSizeMB];
            int sampledGetCount = 0;
            std::vector<long long> latencies_;
            for (LogEntry* entry : logEntries) {
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
                            if (dramBirth + dlLat <= timestamp) { // hit from DRAM
                                latencies_.push_back(dramLat);
                            } else { // hit from DRAM, but waiting for data, and miss from OSC
                                latencies_.push_back(dlLat - (timestamp - dramBirth));
                            }
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

            double avgLatency = 0.;
            for (long long lat : latencies_) {
                avgLatency += (double) lat / (double) latencies_.size();
            }

            return std::make_pair(sampledGetCount, (long long) avgLatency);
        }));
    }

    int sampledGetCount = 0;
    std::vector<long long>* latencies_ = new std::vector<long long>();
    for (int i = 0; i < miniCacheCount; i++) {
        std::pair<int, long long> result = futures[i].get();
        sampledGetCount = result.first;
        long long avgLatency = result.second;
        latencies_->push_back(avgLatency);
    }
    minutes.push_back(minute);
    sampledGetCounts.push_back(sampledGetCount);
    latencies.push_back(latencies_);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    for (int i = 0; i < miniCacheCount; i++) {
        int cacheSizeMB = cacheSizeUnitMB * (i + 1);
        writeLineToFile(statFilePath, std::to_string(minute) + "," + std::to_string(cacheSizeMB) + "," + std::to_string((*latencies_)[i]), true);
    }
    writeLineToFile(reqCountFilePath, std::to_string(minute) + "," + std::to_string(logEntries.size()) + "," + std::to_string(sampledGetCount), true);
}

void LatencyMiniatureSimulation::updateOSCSize(long long oscSizeByte) {
    for (auto it = oscCaches.begin(); it != oscCaches.end(); it++) {
        it->second->updateCacheSize(oscSizeByte);
    }
}

void LatencyMiniatureSimulation::getCacheSizeMBs(int cacheSizeMBs[]) {
    for (int i = 0; i < miniCacheCount; i++) {
        cacheSizeMBs[i] = cacheSizeUnitMB * (i + 1);
    }
}

void LatencyMiniatureSimulation::getLatencies(long long latencies_[]) {
    int monitoredTime = minutes[minutes.size() - 1];
    std::cout << "End minute: " << minutes[minutes.size() - 1] << ", Monitored time: " << monitoredTime << " minutes" << std::endl;
    std::cout << "Warmup time: " << WARMUP_TIME_MINUTE << " minutes" << std::endl;
    if (monitoredTime < WARMUP_TIME_MINUTE) {
        std::fill(latencies_, latencies_ + miniCacheCount, -1LL);
    } else {
        for (int i = 0; i < miniCacheCount; i++) {
            latencies_[i] = (*(latencies[latencies.size() - 1]))[i];
        }
    }
}

void LatencyMiniatureSimulation::getRequestCount(int& latencyRequestCount) {
    latencyRequestCount = totalRequestCounts[totalRequestCounts.size() - 1];
}

void LatencyMiniatureSimulation::getSampledGetCount(int& sampledGetCount) {
    sampledGetCount = sampledGetCounts[sampledGetCounts.size() - 1];
}

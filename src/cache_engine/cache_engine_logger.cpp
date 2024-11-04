#include "cache_engine_logger.hpp"
#include "enums.hpp"
// #include "utils.hpp"

#include <rocksdb/db.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <crypto++/sha.h>
#include <crypto++/hex.h>

CacheEngineLogger::CacheEngineLogger(const std::string& logDirPath_) : bufferCnt(100), bufferSize(1000), activeBuffer(0), bufferRound(0) {
    buffers = new LogEntry*[bufferCnt];
    for (int i = 0; i < bufferCnt; i++) {
        buffers[i] = new LogEntry[bufferSize];
    }
    bufferIdx = new int[bufferCnt];
    for (int i = 0; i < bufferCnt; i++) {
        bufferIdx[i] = 0;
    }
    flushings = new bool[bufferCnt];
    for (int i = 0; i < bufferCnt; i++) {
        flushings[i] = false;
    }

    logDirPath = logDirPath_;
    std::string rmCommand = "rm -rf " + logDirPath;
    assert(system(rmCommand.c_str()) == 0);
    std::string mkdirCommand = "mkdir -p " + logDirPath;
    assert(system(mkdirCommand.c_str()) == 0);
}

void CacheEngineLogger::log_request(long long timestamp, const CloudOperation opType, const std::string& key, size_t size) {
    bufferMutex.lock();
    while (flushings[activeBuffer])
        ;
    buffers[activeBuffer][bufferIdx[activeBuffer]].timestamp = timestamp;
    buffers[activeBuffer][bufferIdx[activeBuffer]].opType = opType;
    buffers[activeBuffer][bufferIdx[activeBuffer]].key = key;
    buffers[activeBuffer][bufferIdx[activeBuffer]].size = size;
    bufferIdx[activeBuffer]++;
    if (bufferIdx[activeBuffer] == bufferSize) {
        std::thread flushThread(&CacheEngineLogger::manualFlush, this, activeBuffer, bufferRound);
        flushThread.detach();
        flushings[activeBuffer] = true;
        activeBuffer = (activeBuffer + 1) % bufferCnt;
        if (activeBuffer == 0) {
            bufferRound++;
        }
    }
    bufferMutex.unlock();
}

void CacheEngineLogger::flush(std::vector<std::string>& logFilePaths) {
    bufferMutex.lock();
    int lastBufferIdx = activeBuffer, lastBufferRound = bufferRound;
    std::thread flushThread(&CacheEngineLogger::manualFlush, this, activeBuffer, bufferRound);
    flushings[activeBuffer] = true;
    activeBuffer = (activeBuffer + 1) % bufferCnt;
    if (activeBuffer == 0) {
        bufferRound++;
    }
    flushThread.join();
    bufferMutex.unlock();

    try {
        int x, y;
        for (const auto& entry: std::filesystem::directory_iterator(logDirPath)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (sscanf(filename.c_str(), "%d_%d.bin", &x, &y) == 2) {
                    if (x < lastBufferRound || (x == lastBufferRound && y <= lastBufferIdx)) {
                        logFilePaths.push_back(entry.path().string());
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to list files in " << logDirPath << ": " << e.what() << std::endl;
    }
}

void CacheEngineLogger::manualFlush(int flushBufferIdx, int flushBufferRound) {
    std::string logFilePath = logDirPath + "/" + std::to_string(flushBufferRound) + "_" + std::to_string(flushBufferIdx) + ".bin";
    std::ofstream logFile(logFilePath, std::ios::binary | std::ios::app);
    if (logFile.is_open()) {
        for (int i = 0; i < bufferIdx[flushBufferIdx]; i++) {
            writeLogEntry(logFile, buffers[flushBufferIdx][i]);
        }
        logFile.close();
    } else {
        std::cerr << "Failed to open log file: " << logFilePath << std::endl;
    }
    bufferIdx[flushBufferIdx] = 0;
    flushings[flushBufferIdx] = false;
}

void writeLogEntry(std::ofstream& outFile, const LogEntry& entry) {
    outFile.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
    int opType = static_cast<int>(entry.opType);
    outFile.write(reinterpret_cast<const char*>(&opType), sizeof(opType));

    size_t keySize = entry.key.size();
    outFile.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
    outFile.write(entry.key.c_str(), keySize);

    outFile.write(reinterpret_cast<const char*>(&entry.size), sizeof(entry.size));
}

LogEntry* readLogEntry(std::ifstream& inFile) {
    LogEntry* entry = new LogEntry();
    inFile.read(reinterpret_cast<char*>(&entry->timestamp), sizeof(entry->timestamp));
    int opType;
    inFile.read(reinterpret_cast<char*>(&opType), sizeof(opType));
    entry->opType = static_cast<CloudOperation>(opType);

    size_t keySize;
    inFile.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
    entry->key.resize(keySize);
    inFile.read(&entry->key[0], keySize);

    inFile.read(reinterpret_cast<char*>(&entry->size), sizeof(entry->size));

    return entry;
}

void readCacheEngineLogFile(const std::string& logFilePath, std::vector<LogEntry*>& logEntries) {
    std::ifstream logFile(logFilePath, std::ios::binary);
    if (logFile.is_open()) {
        while (logFile.peek() != EOF) {
            logEntries.push_back(readLogEntry(logFile));
        }
        logFile.close();
    } else {
        throw std::runtime_error("Failed to open log file: " + logFilePath);
    }
}

void readCacheEngineLogFileWithSampling(const std::string& logFilePath, std::vector<LogEntry*>& logEntries, int hashSpace, int sampledHashSpace) {
    std::ifstream logFile(logFilePath, std::ios::binary);
    if (logFile.is_open()) {
        while (logFile.peek() != EOF) {
            LogEntry* entry = readLogEntry(logFile);
            std::string key = entry->key;
            if (hashIdC(key, hashSpace) > sampledHashSpace) {
                delete entry;
                continue;
            }
            logEntries.push_back(entry);
        }
        logFile.close();
    } else {
        throw std::runtime_error("Failed to open log file: " + logFilePath);
    }
}


std::string byteArrayToHexStringC(const uint8_t* array, size_t size) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i) {
        ss << std::setw(2) << static_cast<unsigned int>(array[i]);
    }
    return ss.str();
}


int hashIdC(std::string& key, int hashSize) {
    CryptoPP::SHA1 sha1;
    uint8_t digest[CryptoPP::SHA1::DIGESTSIZE];
    sha1.CalculateDigest(digest, reinterpret_cast<const byte*>(key.c_str()), key.length());
    std::string digested = byteArrayToHexStringC(digest, CryptoPP::SHA1::DIGESTSIZE);
    digested = digested.substr(0, 15);
    int hashed = std::stoll(digested, nullptr, 16) % hashSize;
    return hashed;
}


int readCacheEngineLogFileWithSamplingByChunk(const std::string& logFilePath, std::vector<LogEntry*>& logEntries, int hashSpace, int sampledHashSpace) {
    std::ifstream logFile(logFilePath, std::ios::binary);
    logFile.seekg(0, std::ios::end);
    std::streampos filesize = logFile.tellg();
    logFile.seekg(0, std::ios::beg);
    
    char* buffer = new char[filesize];
    logFile.read(buffer, filesize);

    size_t pos = 0;
    int requestCount = 0;
    while (pos < filesize) {
        requestCount++;
        long long timestamp;
        int opType;
        std::string key;
        size_t keySize, size;

        std::memcpy(&keySize, buffer + pos + sizeof(timestamp) + sizeof(opType), sizeof(keySize));
        key.resize(keySize);
        std::memcpy(&key[0], buffer + pos + sizeof(timestamp) + sizeof(opType) + sizeof(keySize), keySize);
        if (hashIdC(key, hashSpace) > sampledHashSpace) {
            pos += sizeof(timestamp) + sizeof(opType) + sizeof(keySize) + keySize + sizeof(size);
            continue;
        }
        std::memcpy(&timestamp, buffer + pos, sizeof(timestamp));
        std::memcpy(&opType, buffer + pos + sizeof(timestamp), sizeof(opType));
        std::memcpy(&size, buffer + pos + sizeof(timestamp) + sizeof(opType) + sizeof(keySize) + keySize, sizeof(size));
        logEntries.push_back(new LogEntry{timestamp, static_cast<CloudOperation>(opType), key, size});

        pos += sizeof(timestamp) + sizeof(opType) + sizeof(keySize) + keySize + sizeof(size);
    }
    logFile.close();
    delete[] buffer;
    
    return requestCount;
}


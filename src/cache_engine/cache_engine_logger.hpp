#ifndef CACHEENGINELOGGER_H
#define CACHEENGINELOGGER_H

#include <mutex>
#include <string>
#include <vector>

#include "enums.hpp"

struct LogEntry {
    long long timestamp;
    CloudOperation opType;
    std::string key;
    size_t size;
};

class CacheEngineLogger {
public:
    CacheEngineLogger(const std::string& log_file_path);
    void log_request(long long timestamp, const CloudOperation opType, const std::string& key, size_t size);
    void flush(std::vector<std::string>& logFilePaths);
    void manualFlush(int flushBufferIdx, int flushBufferRound);

private:
    LogEntry** buffers;
    int* bufferIdx;
    bool* flushings;
    int bufferCnt;
    int bufferSize;
    int activeBuffer;
    int bufferRound;
    std::string logDirPath;
    std::mutex bufferMutex;
};

void writeLogEntry(std::ofstream& outFile, const LogEntry& entry);
LogEntry* readLogEntry(std::ifstream& inFile);
void readCacheEngineLogFile(const std::string& logFilePath, std::vector<LogEntry*>& logEntries);
void readCacheEngineLogFileWithSampling(const std::string& logFilePath, std::vector<LogEntry*>& logEntries, int hashSpace, int sampledHashSpace);

std::string byteArrayToHexStringC(const uint8_t* array, size_t size);
int hashIdC(std::string& key, int hashSize);
int readCacheEngineLogFileWithSamplingByChunk(const std::string& logFilePath, std::vector<LogEntry*>& logEntries, int hashSpace, int sampledHashSpace);

#endif  // CACHEENGINELOGGER_H

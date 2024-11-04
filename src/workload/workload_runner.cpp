// workload_runner 

#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <chrono>
#include <string>

#include "workload_runner.hpp"
#include "macaron_client.hpp"
#include "configs.hpp"
#include "cache_engine_logger.hpp"
#include "enums.hpp"


RequestProcessor::RequestProcessor(std::string mbakery_address, size_t maxSize, size_t nThreads, bool logLatency) {
    client = new MacaronClient();
    if (logLatency) {
        client->enableLatencyLogging();
        client->startLoggingLatency(60);
    }
    client->connect(mbakery_address);
    client->TriggerOptimization();
    maxQueueSize = maxSize;
    numThreads = nThreads;
}

void RequestProcessor::addRequest(LogEntry* entry) {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return requests.size() < maxQueueSize; });
    requests.push(entry);
    cv.notify_all();
}

void RequestProcessor::processRequests() {
    startTimestamp = std::chrono::system_clock::now();
    for (size_t i = 0; i < numThreads; ++i) {
        workerThreads.push_back(new std::thread(&RequestProcessor::worker, this));
    }
}

void RequestProcessor::stopProcessing() {
    allRequestsSent = true;
    cv.notify_all();

    for (std::thread* workerThread : workerThreads) {
        workerThread->join();
    }
    client->StopOptimization();
    client->waitLatencyLogging();
}

void RequestProcessor::worker() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]() { return !requests.empty() || allRequestsSent; });

        if (requests.empty() && allRequestsSent) {
            return;
        }

        if (!requests.empty()) {
            auto request = requests.front(); 
            bool processRequest = false;
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - startTimestamp);
            if (duration.count() > request->timestamp) {
                processRequest = true;
                requests.pop();
            }
            lock.unlock();
            cv.notify_all();
            std::string response;
            if (processRequest) {
                switch (request->opType) {
                    case PUT:
                        client->Put(request->key, std::string(request->size, 'a'));
                        break;
                    case GET:
                        client->Get(request->key, response);
                        break;
                    case DELETE:
                        client->Delete(request->key);
                        break;
                    default:
                        std::cerr << "Invalid operation type" << std::endl;
                        break;
                }
            }
        }
    }
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc("Workload preparation options");
    desc.add_options()
        ("expname", boost::program_options::value<std::string>(), "Name of the experiment")
        ("tracepath", boost::program_options::value<std::string>(), "Path to the trace file")
        ("mbakery", boost::program_options::value<std::string>()->default_value("localhost:50051"), "Address of the macaron bakery server")
        ("loglatency", boost::program_options::value<bool>()->default_value(false), "Whether to log latency or not");
    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("expname") == 0 || vm.count("tracepath") == 0) {
        std::cerr << "Usage: workload_prep --expname=<expname> --tracepath=<tracepath> --mbakery=<mbakery>" << std::endl;
        return 1;
    }

    std::string mbakery_address = vm.count("mbakery") ? vm["mbakery"].as<std::string>() : "localhost:50051";
    std::string expname = vm["expname"].as<std::string>();
    std::string tracepath = vm["tracepath"].as<std::string>();
    bool logLatency = vm["loglatency"].as<bool>();
    std::cout << "Experiment name: " << expname << std::endl;
    std::cout << "Trace path: " << tracepath << std::endl;
    std::cout << "Logging latency: " << logLatency << std::endl;

    RequestProcessor processor(mbakery_address, 1000, 32, logLatency);
    processor.processRequests();

    std::ifstream bin_traceFile_chunk(tracepath, std::ifstream::binary);
    int chunk_size = 24 * 1024;
    char* buffer = new char[chunk_size];

    bin_traceFile_chunk.seekg(0, std::ios::end);
    std::streampos filesize = bin_traceFile_chunk.tellg();
    bin_traceFile_chunk.seekg(0, std::ios::beg);

    int requestCnt = 0;
    while (filesize > 0) {
        size_t chunkSize = std::min(filesize, static_cast<std::streampos>(chunk_size));
        bin_traceFile_chunk.read(buffer, chunkSize);
        for (size_t i = 0; i < chunkSize; i += 24) {
            long long timestamp;
            int opType;
            int key;
            long long size;
            std::memcpy(&timestamp, buffer + i, sizeof(timestamp));
            std::memcpy(&opType, buffer + i + 8, sizeof(opType));
            std::memcpy(&key, buffer + i + 12, sizeof(key));
            std::memcpy(&size, buffer + i + 16, sizeof(size));
            
            LogEntry* entry = new LogEntry();
            entry->timestamp = timestamp;
            entry->opType = opType == 0 ? PUT : (opType == 1 ? GET : DELETE);
            entry->key = std::to_string(key);
            entry->size = size;
            processor.addRequest(entry);
            requestCnt++;
        }
        filesize -= chunkSize;
    }
    bin_traceFile_chunk.close();
    delete[] buffer;

    std::this_thread::sleep_for(std::chrono::seconds(5));
    processor.stopProcessing(); // Stop processing requests

    std::cout << "Processed " << requestCnt << " requests" << std::endl;

    return 0;
}
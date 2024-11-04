#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <string>

#include "cloudapis.hpp"
#include "configs.hpp"

int main(int argc, char** argv) {
    boost::program_options::options_description desc("Workload preparation options");
    desc.add_options()
        ("expname", boost::program_options::value<std::string>(), "Name of the experiment")
        ("tracepath", boost::program_options::value<std::string>(), "Path to the trace file")
        ("is_on_prem", boost::program_options::value<bool>(), "Whether the cloud storage is on-prem or not")
        ("region", boost::program_options::value<std::string>(), "Region of the cloud storage");
    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("expname") == 0 || vm.count("tracepath") == 0) {
        std::cerr << "Usage: workload_prep --expname=<expname> --tracepath=<tracepath>" << std::endl;
        return 1;
    }

    std::string expname = vm["expname"].as<std::string>();
    std::string tracepath = vm["tracepath"].as<std::string>();
    bool isOnPrem = vm.count("is_on_prem") > 0 ? vm["is_on_prem"].as<bool>() : false;
    std::string region = vm.count("region") > 0 ? vm["region"].as<std::string>() : "us-east-1";

    std::cout << "Experiment name: " << expname << std::endl;
    std::cout << "Trace path: " << tracepath << std::endl;

    std::cout << "Find the max key value" << std::endl;
    std::ifstream bin_traceFile_chunk(tracepath, std::ifstream::binary);
    int chunk_size = 24 * 1024;
    char* buffer = new char[chunk_size];

    bin_traceFile_chunk.seekg(0, std::ios::end);
    std::streampos filesize = bin_traceFile_chunk.tellg();
    size_t totalFileSize = filesize;

    // first, find the max key id
    bin_traceFile_chunk.seekg(0, std::ios::beg);
    int max_key = 0;
    while (filesize > 0) {
        size_t chunkSize = std::min(filesize, static_cast<std::streampos>(chunk_size));
        bin_traceFile_chunk.read(buffer, chunkSize);
        for (size_t i = 0; i < chunkSize; i += 24) {
            int key;
            std::memcpy(&key, buffer + i + 12, sizeof(key));
            if (key > max_key) {
                max_key = key;
            }
        }
        filesize -= chunkSize;
    }
    std::cout << "Max key: " << max_key << std::endl;

    // start generating the initial bucket state
    std::cout << "Start generating the initial bucket state" << std::endl;
    long long* dataStorage = new long long[max_key + 1];
    std::fill(dataStorage, dataStorage + max_key + 1, -1L);

    bin_traceFile_chunk.seekg(0, std::ios::beg);
    filesize = totalFileSize;
    size_t progress = 0, next_print = totalFileSize / 20;
    int totalRequestCount = 0;
    std::map<int, long long> keySizeMap;
    while (filesize > 0) {
        size_t chunkSize = std::min(filesize, static_cast<std::streampos>(chunk_size));
        bin_traceFile_chunk.read(buffer, chunkSize);
        for (size_t i = 0; i < chunkSize; i += 24) {
            int opType;
            int key;
            long long size;
            std::memcpy(&opType, buffer + i + 8, sizeof(opType));
            std::memcpy(&key, buffer + i + 12, sizeof(key));
            std::memcpy(&size, buffer + i + 16, sizeof(size));
            if (opType == 1 && dataStorage[key] == -1L) {
                keySizeMap[key] = size;
                dataStorage[key] = size;
            } else if (opType == 0) {
                dataStorage[key] = size;
            }
            totalRequestCount++;
        }
        filesize -= chunkSize;
        
        progress += chunkSize;
        if (progress >= next_print) {
            std::cout << "Progress: " << (int) ((double) progress / totalFileSize * 100) << "%" << std::endl;
            next_print += totalFileSize / 20;
        }
    }
    bin_traceFile_chunk.close();
    delete[] buffer;

    std::cout << "Total request count: " << totalRequestCount << std::endl;
    std::cout << "Total number of objects to be written initially " << keySizeMap.size() << std::endl;
    
    std::cout << "Start putting data to the S3 data lake" << std::endl;
    CloudClient* client = new CloudClient();
    DL_BUCKET_NAME = DL_BUCKET_NAME + "-" + expname + "-" + region;
    if (isOnPrem)
        client->ConnectS3OnPrem("http://localhost:9000");
    else
        client->ConnectS3Cloud(region, true);

    if (client->DoesBucketExist(DL_BUCKET_NAME)) {
        std::cout << "Bucket already exists. Listing the keys..." << std::endl;
        std::vector<std::string> existingKeys;
        client->ListBucket(DL_BUCKET_NAME, existingKeys);
        std::cout << "Number of existing keys: " << existingKeys.size() << std::endl;
        for (auto key : existingKeys) {
            int keyInt = std::stoi(key);
            if (keySizeMap.find(keyInt) != keySizeMap.end()) {
                keySizeMap.erase(keyInt);
            }
        }
        std::cout << "Number of keys to be newly written: " << keySizeMap.size() << std::endl;
    } else {
        client->CreateBucket(DL_BUCKET_NAME);
    }

    int Nthreads = 160, done = 0;
    std::vector<std::thread*> threads;
    for (auto key : keySizeMap) {
        threads.push_back(new std::thread([=]() {
            client->PutData(DL_BUCKET_NAME, std::to_string(key.first), std::string(key.second, 'a'));
        }));
        if (threads.size() >= Nthreads) {
            for (auto t: threads) {
                t->join();
                delete t;
            }
            threads.clear();
            std::cout << "Progress: " << done << " / " << keySizeMap.size() << std::endl;
        }
        done++;
    }
    if (threads.size() > 0) {
        for (auto t: threads) {
            t->join();
            delete t;
        }
        threads.clear();
    }
}
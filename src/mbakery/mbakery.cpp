#include <iostream>
#include <string>
#include <vector>
#include <signal.h>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <memory>
#include <filesystem>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <boost/program_options.hpp>

#include "configs.hpp"
#include "meringue.grpc.pb.h"
#include "cache_engine.grpc.pb.h"
#include "configs.hpp"
#include "enums.hpp"
#include "utils.hpp"
#include "mbakery.hpp"
#include "cloudapis.hpp"
#include "cost_minisim.hpp"
#include "latency_minisim.hpp"
#include "cache_engine_logger.hpp"

std::string NAME = "MBakery";
std::string EXP_NAME = "EXPERIMENT_NAME";
Logger* logger;


MBakeryServiceImpl::MBakeryServiceImpl(std::vector<std::string>& _cacheEngineAddresses, std::string& _meringueAddress) : cacheEngineAddresses(_cacheEngineAddresses), meringueAddress(_meringueAddress) {
}

grpc::Status MBakeryServiceImpl::Connect(grpc::ServerContext* context, const mbakery::ConnectRequest* request, mbakery::ConnectResponse* response) {
    std::string peer = context->peer();
    if (CACHE_LEVEL_MODE == SINGLE_LEVEL) {
        response->set_message(cacheEngineAddresses[0]);
    } else if (CACHE_LEVEL_MODE == TWO_LEVEL) {
        response->set_message(concatString(cacheEngineAddresses));
    } else {
        response->set_message("Invalid cache level");
    }
    return grpc::Status::OK;
}

grpc::Status MBakeryServiceImpl::OptTrigger(grpc::ServerContext* context, const mbakery::OptTriggerRequest* request, mbakery::OptTriggerResponse* response) {
    logger->log_message(NAME, "Received OptTrigger request");
    if (!optProcessed) {
        response->set_success(true);

        if (!std::filesystem::exists(MBAKERY_STAT_SUMMARY_FILEPATH)) {
            std::filesystem::create_directories(MBAKERY_STAT_SUMMARY_FILEPATH);
        }
        MBAKERY_STAT_SUMMARY_FILEPATH = MBAKERY_STAT_SUMMARY_FILEPATH + "/mbakery_stat_summary.csv";
        if (std::filesystem::exists(MBAKERY_STAT_SUMMARY_FILEPATH)) {
            std::filesystem::remove(MBAKERY_STAT_SUMMARY_FILEPATH);
        }
        std::ofstream statSummaryFile(MBAKERY_STAT_SUMMARY_FILEPATH, std::ofstream::out);
        statSummaryFile << "Minute,Component,Name,OSCPutCnt,OSCGetCnt,DLGetBytes,DLPutBytes,PrvOptOSCSizeMB,NxtOptOSCSizeMB,PrvOSCCacheSizeBytes,NxtOSCCacheSizeBytes,PrvTotalDataSizeBytes,NxtTotalDataSizeBytes\n";
        statSummaryFile.close();


        start_cache_engine_flush_thread(cacheEngineAddresses, meringueAddress);

        optProcessed = true;
        optTriggered = true;
    } else {
        response->set_success(false);
    }
    return grpc::Status::OK;
}

bool optStop = false;
grpc::Status MBakeryServiceImpl::OptStop(grpc::ServerContext* context, const mbakery::OptStopRequest* request, mbakery::OptStopResponse* response) {
    logger->log_message(NAME, "Received OptStop request");
    if (optTriggered) {
        response->set_success(true);
        optStop = true;
        optTriggered = false;
    } else {
        response->set_success(false);
    }
    return grpc::Status::OK;
}


std::thread *meringueThread;
long long oscSizeMB = INITIAL_OSC_SIZE_MB;
long long dramSizeMB = INITIAL_DRAM_SIZE_MB;
void start_meringue_server(std::string& meringueIpAddress) {
    logger->log_message(NAME, "Starting Meringue server: " + meringueIpAddress);
    std::string localBinFilePath = MACARON_BIN_DIR_PATH + "/meringue";
    if (!send_file(localBinFilePath, REMOTE_BIN_DIR_PATH, meringueIpAddress)) {
        throw std::runtime_error("Failed to send Meringue binary file to " + meringueIpAddress);
    }

    std::string remoteBinFilePath = REMOTE_BIN_DIR_PATH + "/meringue";
    meringueThread = new std::thread([meringueIpAddress, remoteBinFilePath]() {
        std::string command = "ssh -o StrictHostKeyChecking=no " + meringueIpAddress + " \"bash -c '" + remoteBinFilePath + " > /tmp/meringue_error.log 2>&1'\"";
        std::system(command.c_str());
    });
    meringueThread->detach();
}

void init_meringue_server(std::string& meringueAddress) {
    logger->log_message(NAME, "Initializing Meringue server: " + meringueAddress);
    try {
        while (true) {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(meringueAddress, grpc::InsecureChannelCredentials());
            std::unique_ptr<meringue::MeringueService::Stub> stub = meringue::MeringueService::NewStub(channel);
            meringue::InitializeRequest request;
            request.set_contd(false);
            request.set_packing_mode((int) PACKING_MODE);
            request.set_osc_bucket_name(OSC_BUCKET_NAME);
            request.set_log_file_path(MBAKERY_ACCESS_LOG_FILE_PATH);
            request.set_meringue_db_path(MERINGUE_DB_PATH);
            request.set_local_endpoint_url(LOCAL_ENDPOINT_URL);
            request.set_gc_threshold(MERINGUE_GC_THRESHOLD);
            request.set_is_on_prem(IS_ON_PREM);
            request.set_local_region(LOCAL_REGION);

            grpc::ClientContext context;
            meringue::InitializeResponse response;
            grpc::Status status = stub->Initialize(&context, request, &response);

            if (status.ok()) {
                if (response.is_running()) {
                    logger->log_message(NAME, "Meringue server is initialized successfully.");
                    break;
                } else {
                    logger->log_message(NAME, "Meringue server is not running.");
                }
            } else {
                logger->log_message(NAME, "Failed to initialize Meringue server: " + status.error_message());
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
            logger->log_message(NAME, "Retrying to initialize Meringue server...");
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Exception occurred while initializing Meringue server: " + std::string(e.what()));
    }
}

std::map<std::string, std::string> cacheEngineAddressToName;
std::map<std::string, std::string> cacheEngineNameToId;
int cacheEngineIdx = 0;
std::string generate_cache_engine_name(int id) {
    return "CE" + std::to_string(id);
}

CloudClient *rmClient;
void launch_cache_engines(std::vector<std::string>& cacheEngineAddresses, int n) {
    std::vector<std::string> instanceIds = rmClient->createInstances(n);
    std::vector<std::string> ipAddresses;
    for (int i = 0; i < n; i++) {
        std::string cacheEngineName = generate_cache_engine_name(++cacheEngineIdx);
        std::string ipAddress = rmClient->getIpAddresses(instanceIds)[i];
        cacheEngineAddressToName[ipAddress + ":50053"] = cacheEngineName;
        cacheEngineNameToId[cacheEngineName] = instanceIds[i];
        ipAddresses.push_back(ipAddress);
    }

    std::vector<std::thread*> threads;
    for (int i = 0; i < n; i++) {
        std::string ipAddress = ipAddresses[i];
        threads.push_back(new std::thread([ipAddress]() {
            std::string mountScriptPath = std::string(getenv("HOME")) + "/mount.sh";
            std::string command = "ssh -o StrictHostKeyChecking=no " + ipAddress + " \"bash -c '" + mountScriptPath + "'\"";
            std::system(command.c_str());
        }));
    }

    for (auto thread : threads)
        thread->join();
    for (auto thread : threads)
        delete thread;
    threads.clear();

    for (std::string ipAddress : ipAddresses)
        cacheEngineAddresses.push_back(ipAddress + ":50053");
}

std::vector<std::thread*> cacheEngineThreads;
void start_cache_engines(std::vector<std::string>& cacheEngineAddresses) {
    logger->log_message(NAME, "Starting cache engines");
    for (std::string cacheEngineAddress : cacheEngineAddresses) {
        cacheEngineThreads.push_back(new std::thread([cacheEngineAddress]() {
            std::string cacheEngineIpAddress;
            int cacheEnginePort;
            if (!splitIpAddressAndPort(cacheEngineAddress, cacheEngineIpAddress, cacheEnginePort)) {
                throw std::runtime_error("Invalid Cache Engine address: " + cacheEngineAddress);
            }

            std::string localBinFilePath = MACARON_BIN_DIR_PATH + "/cache_engine";
            if (!send_file(localBinFilePath, REMOTE_BIN_DIR_PATH, cacheEngineIpAddress)) {
                throw std::runtime_error("Failed to send cache engine binary file to " + cacheEngineIpAddress);
            }

            if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                std::string localRedisConfFilePath = MACARON_CONF_DIR_PATH + "/redis.conf";
                if (!send_file(localRedisConfFilePath, REMOTE_CONF_DIR_PATH, "redis.conf", cacheEngineIpAddress)) {
                    throw std::runtime_error("Failed to send Redis configuration file to " + cacheEngineIpAddress);
                }
            }

            std::string remoteBinFilePath = REMOTE_BIN_DIR_PATH + "/cache_engine";

            std::string command = "ssh -o StrictHostKeyChecking=no " + cacheEngineIpAddress + " \"bash -c '" + remoteBinFilePath + " > /tmp/cache_engine_error.log 2>&1'\"";
            std::system(command.c_str());
        }));
        cacheEngineThreads.back()->detach();
    }
}

void init_cache_engines(std::vector<std::string>& cacheEngineAddresses, std::string& meringueAddress) {
    logger->log_message(NAME, "Initializing cache engines");
    std::vector<std::thread*> threads;
    for (std::string cacheEngineAddress : cacheEngineAddresses) {
        threads.push_back(new std::thread([cacheEngineAddress, meringueAddress]() {
            try {
                std::string cacheEngineName;
                if (IS_ON_PREM || CACHE_LEVEL_MODE == SINGLE_LEVEL) {
                    cacheEngineName = generate_cache_engine_name(++cacheEngineIdx);
                } else {
                    cacheEngineName = cacheEngineAddressToName[cacheEngineAddress];
                }
                while (true) {
                    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(cacheEngineAddress, grpc::InsecureChannelCredentials());
                    std::unique_ptr<cacheengine::CacheEngineService::Stub> stub = cacheengine::CacheEngineService::NewStub(channel);
                    cacheengine::InitializeRequest request;
                    request.set_dl_csp((int) DL_CSP);
                    request.set_dl_bucket_name(DL_BUCKET_NAME);
                    request.set_osc_csp((int) LOCAL_CSP);
                    request.set_osc_bucket_name(OSC_BUCKET_NAME);
                    request.set_meringue_address(meringueAddress);
                    request.set_cache_level_mode((int) CACHE_LEVEL_MODE);
                    request.set_packing_mode((int) PACKING_MODE);
                    request.set_log_file_path(CACHE_ENGINE_LOG_FILE_PATH + "/" + cacheEngineName);
                    request.set_local_endpoint_url(LOCAL_ENDPOINT_URL);
                    request.set_remote_endpoint_url(REMOTE_ENDPOINT_URL);
                    request.set_max_block_size(MAX_BLOCK_SIZE);
                    request.set_max_block_object_count(MAX_BLOCK_OBJECT_COUNT);
                    request.set_name(cacheEngineName);
                    if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                        request.set_redis_conf_file_path(REMOTE_CONF_DIR_PATH + "/redis.conf");
                    }
                    request.set_latency_logging_enabled(LATENCY_LOGGING_ENABLED);
                    request.set_is_on_prem(IS_ON_PREM);
                    request.set_local_region(LOCAL_REGION);
                    request.set_remote_region(REMOTE_REGION);

                    grpc::ClientContext context;
                    cacheengine::InitializeResponse response;
                    grpc::Status status = stub->Initialize(&context, request, &response);
                    
                    if (status.ok()) {
                        if (response.is_running()) {
                            logger->log_message(NAME, "Cache Engine is initialized successfully.");
                            break;
                        } else {
                            logger->log_message(NAME, "Cache Engine is not running.");
                        } 
                    } else {
                        logger->log_message(NAME, "Failed to initialize Cache Engine: " + status.error_message());
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    logger->log_message(NAME, "Retrying to initialize Cache Engine...");
                }
            } catch (const std::exception& e) {
                throw std::runtime_error("Exception occurred while initializing cache engine: " + std::string(e.what()));
            }
        }));
    }

    for (auto thread : threads)
        thread->join();
    for (auto thread : threads)
        delete thread;
    threads.clear();
}

int computeNewDRAMSizeMB(int dramSizeMB, int scaleInCounter) {
    if (scaleInCounter >= DRAM_SCALE_IN_THRESHOLD) {
        scaleInCounter = 0;
        int nodeCount = (dramSizeMB + DRAM_UNIT_SIZE_MB - 1) / DRAM_UNIT_SIZE_MB;
        if (nodeCount > 1) {
            nodeCount /= 2;
            return nodeCount * DRAM_UNIT_SIZE_MB;
        } else {
            return DRAM_UNIT_SIZE_MB;
        }
    }
    return dramSizeMB;
}

int findKneePoint(int cacheSizeMBs[], long long elc[], int length) {
    if (length == 1)
        return cacheSizeMBs[0];
    assert(length >= 3);

    long long x1 = cacheSizeMBs[0], x2 = cacheSizeMBs[length - 1];
    long long y1 = elc[0], y2 = elc[length - 1];
    std::cout << "x1: " << x1 << ", y1: " << y1 << ", x2: " << x2 << ", y2: " << y2 << std::endl;

    double maxDistance = -1;
    int kneePoint = cacheSizeMBs[0];
    long long kneePointValue = elc[0];

    std::cout << "Finding knee point" << std::endl;
    for (int i = 0; i < length; i++) {
        long long x = cacheSizeMBs[i];
        long long y = elc[i];
        double numerator = abs((y2 - y1) * x - (x2 - x1) * y + x2 * y1 - y2 * x1);
        double denominator = sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        double distance = numerator / denominator;
        std::cout << "Before sqrt: " << (y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1) << std::endl;
        std::cout << "After sqrt: " << sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1)) << std::endl;
        std::cout << "numerator: " << numerator << ", denominator: " << denominator << std::endl;
        std::cout << "x: " << x << ", y: " << y << ", distance: " << distance << std::endl;

        if (distance > maxDistance) {
            maxDistance = distance;
            kneePoint = x;
            kneePointValue = y;
        }
    }

    for (int i = 0; i < length; i++) {
        if (elc[i] <= kneePointValue) {
            return cacheSizeMBs[i];
        }
    }

    return kneePoint;
}

long long optimizeDRAMSizeMB(int cacheSizeMBs[], long long elc[], int length, long long latencyTarget) {
    for (int i = 0; i < length; i++) {
        if (elc[i] > -1LL && elc[i] < latencyTarget) {
            return cacheSizeMBs[i];
        }
    }
    return findKneePoint(cacheSizeMBs, elc, length);
}

std::thread *cacheEngineFlushThread;
int dramScaleInCounter;
void start_cache_engine_flush_thread(std::vector<std::string>& cacheEngineAddresses, std::string& meringueAddress) {
    logger->log_message(NAME, "Starting cache engine flush thread");
    logger->log_message(NAME, "Warmup time: " + std::to_string(WARMUP_TIME_MINUTE) + " minutes");
    logger->log_message(NAME, "Flush and optimization period: " + std::to_string(CACHE_ENGINE_FLUSH_INTERVAL_SEC) + " seconds");
    cacheEngineFlushThread = new std::thread([&cacheEngineAddresses, meringueAddress]() {
        CostMiniatureSimulation *costMiniatureSimulation;
        LatencyMiniatureSimulation *latencyMiniatureSimulation;
        CloudClient *costMinisimClient, *latencyMinisimClient;
        bool lambdaContd = false;
        if (USE_LAMBDA) {
            costMiniatureSimulation = new CostMiniatureSimulation(COST_SAMPLING_RATIO, COST_MINICACHE_COUNT, COST_CACHE_SIZE_UNIT_MB);
            costMinisimClient = new CloudClient();
            costMinisimClient->ConnectLambda();
            if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                latencyMiniatureSimulation = new LatencyMiniatureSimulation(LATENCY_SAMPLING_RATIO, LATENCY_MINICACHE_COUNT, LATENCY_CACHE_SIZE_UNIT_MB);
                latencyMinisimClient = new CloudClient();
                latencyMinisimClient->ConnectLambda();
                dramScaleInCounter = 0;
            }
        } else {
            costMiniatureSimulation = new CostMiniatureSimulation(COST_SAMPLING_RATIO, COST_MINICACHE_COUNT, COST_CACHE_SIZE_UNIT_MB, COST_DB_BASE_DIR_PATH);
            latencyMiniatureSimulation = new LatencyMiniatureSimulation(LATENCY_SAMPLING_RATIO, LATENCY_MINICACHE_COUNT, LATENCY_CACHE_SIZE_UNIT_MB, oscSizeMB, LATENCY_DB_BASE_DIR_PATH);
        }
        
        auto optInitTime = std::chrono::steady_clock::now();
        auto nextStartTime = std::chrono::steady_clock::now() + std::chrono::seconds(CACHE_ENGINE_FLUSH_INTERVAL_SEC);
        while (!optStop) {
            // Sleep until next start time
            auto sleepTime = nextStartTime - std::chrono::steady_clock::now();
            logger->log_message(NAME, "Sleeping for " + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(sleepTime).count()) + " seconds");
            std::this_thread::sleep_for(sleepTime);

            // Measuring this phase's start time
            auto mainOptimizationStartTime = std::chrono::steady_clock::now();
            
            // Start flushing the cache engines' data access logs
            auto startTime = std::chrono::steady_clock::now();
            nextStartTime = std::chrono::steady_clock::now() + std::chrono::seconds(CACHE_ENGINE_FLUSH_INTERVAL_SEC);
            std::vector<std::thread*> threads;
            std::mutex flushMutex;
            std::map<std::string, std::tuple<int, long long, long long>> cacheEngineStats;
            int totalLogFileCount = 0;
            for (auto& cacheEngineAddress : cacheEngineAddresses) {
                threads.push_back(new std::thread([cacheEngineAddress, &flushMutex, &cacheEngineStats, &totalLogFileCount]() {
                    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(cacheEngineAddress, grpc::InsecureChannelCredentials());
                    std::unique_ptr<cacheengine::CacheEngineService::Stub> stub = cacheengine::CacheEngineService::NewStub(channel);
                    cacheengine::FlushLogRequest request;
                    cacheengine::FlushLogResponse response;
                    grpc::ClientContext context;
                    request.set_cache_name(cacheEngineAddress);
                    request.set_log_dir_path(MBAKERY_ACCESS_LOG_FILE_PATH);
                    grpc::Status status = stub->FlushLog(&context, request, &response);
                    if (!status.ok()) {
                        logger->log_message(NAME, "Failed to flush cache engine: " + status.error_message());
                    }
                    
                    std::lock_guard<std::mutex> lock(flushMutex);
                    cacheEngineStats[cacheEngineAddress] = std::make_tuple(response.osc_put_cnt(), response.dl_get_bytes(), response.dl_put_bytes());
                    totalLogFileCount += response.file_count();
                }));
            }
            for (auto thread : threads) {
                thread->join();
            }
            while (!threads.empty()) {
                delete threads.back();
                threads.pop_back();
            }
            auto elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
            logger->log_message(NAME, "Flushing cache engines took " + std::to_string(elapsedTime) + " seconds");

            // Start running Meringue Cache updating
            logger->log_message(NAME, "Start Meringue Cache updating (background)");
            std::thread oscCacheUpdateThread = std::thread(runOSCUpdateCache, meringueAddress, totalLogFileCount);

            // Start running miniature simulation to generate MRC/BMC and get the expected miss ratio, bytes miss, number of requests
            if (USE_LAMBDA) { // Run miniature simulation using AWS Lambda
                startTime = std::chrono::steady_clock::now();
                long long currentMinute = std::chrono::duration_cast<std::chrono::minutes>(std::chrono::steady_clock::now() - optInitTime).count();

                std::thread latencyThread, costThread;
                if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                    logger->log_message(NAME, "Running latency miniature simulation");
                    latencyThread = std::thread(runLatencyMinisimLambda, latencyMinisimClient, currentMinute, totalLogFileCount, lambdaContd);
                }
                logger->log_message(NAME, "Running cost miniature simulation");
                costThread = std::thread(runCostMinisimLambda, costMinisimClient, currentMinute, totalLogFileCount, lambdaContd);

                if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                    latencyThread.join();
                }
                costThread.join();
                logger->log_message(NAME, "Done running miniature simulations");

                std::string lambdaDirPath;
                if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                    lambdaDirPath = NFS_PATH + "/" + EXP_NAME + "/latency_minisim";
                    latencyMiniatureSimulation->loadLastLambdaData(lambdaDirPath, currentMinute);
                }
                lambdaDirPath = NFS_PATH + "/" + EXP_NAME + "/cost_minisim";
                costMiniatureSimulation->loadLastLambdaData(lambdaDirPath, currentMinute);

                lambdaContd = true;
                elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
                logger->log_message(NAME, "Running miniature simulations took " + std::to_string(elapsedTime) + " seconds");
            } else { // Run miniature simulation in a local machine
                throw new std::runtime_error("Not implemented yet: local optimization is not implemented yet");
            }

            // Optimize the DRAM size and OSC size
            startTime = std::chrono::steady_clock::now();
            long long prvDramSizeMB = dramSizeMB, newDramSizeMB = dramSizeMB;
            if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                int latencyCacheSizeMBs[LATENCY_MINICACHE_COUNT];
                long long elc[LATENCY_MINICACHE_COUNT];
                int latencyRequestCount = 0, sampledLatencyGetCount = 0;

                latencyMiniatureSimulation->getCacheSizeMBs(latencyCacheSizeMBs);
                latencyMiniatureSimulation->getLatencies(elc);
                latencyMiniatureSimulation->getRequestCount(latencyRequestCount);
                latencyMiniatureSimulation->getSampledGetCount(sampledLatencyGetCount);
                
                if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                    if (elc[0] > -1LL) {  // Optimize the DRAM size when it's the right time
                        logger->log_message(NAME, "Optimizing DRAM size");
                        if (latencyRequestCount < LATENCY_MIN_REQ_COUNT) {
                            dramScaleInCounter++;
                            newDramSizeMB = computeNewDRAMSizeMB(dramSizeMB, dramScaleInCounter);
                        } else {
                            if (sampledLatencyGetCount > 0) {
                                newDramSizeMB = optimizeDRAMSizeMB(latencyCacheSizeMBs, elc, LATENCY_MINICACHE_COUNT, LATENCY_TARGET);
                                int prvNodeCount = (dramSizeMB + DRAM_UNIT_SIZE_MB - 1) / DRAM_UNIT_SIZE_MB;
                                int newNodeCount = (newDramSizeMB + DRAM_UNIT_SIZE_MB - 1) / DRAM_UNIT_SIZE_MB;
                                dramScaleInCounter = newNodeCount >= prvNodeCount ? 0 : dramScaleInCounter + 1;
                                if (newNodeCount < prvNodeCount) {
                                    newDramSizeMB = computeNewDRAMSizeMB(dramSizeMB, dramScaleInCounter);
                                }
                            }
                        }
                        logger->log_message(NAME, "Optimized DRAM size: " + std::to_string(newDramSizeMB) + " MB");
                        dramSizeMB = newDramSizeMB;

                    } else {
                        logger->log_message(NAME, "Not enough time pass to run DRAM optimization");
                    }
                }
            }


            int costCacheSizeMBs[COST_MINICACHE_COUNT];
            double mrc[COST_MINICACHE_COUNT];
            long long bmc[COST_MINICACHE_COUNT];
            int getCnt = -1, putCnt = -1;
            float avgBlockPortion = 1.;

            costMiniatureSimulation->getCacheSizeMBs(costCacheSizeMBs);
            costMiniatureSimulation->getMRC(mrc);
            costMiniatureSimulation->getBMC(bmc);
            costMiniatureSimulation->getReqCounts(getCnt, putCnt);

            long long prevOptOSCSizeMB = oscSizeMB, newOptOSCSizeMB = oscSizeMB;
            if (mrc[0] > -0.5) {
                logger->log_message(NAME, "Optimizing OSC size");
                newOptOSCSizeMB = optimizeOSCSizeMB(costCacheSizeMBs, mrc, bmc, getCnt, putCnt, avgBlockPortion);
                logger->log_message(NAME, "Optimized OSC size: " + std::to_string(newOptOSCSizeMB) + " MB");
            } else {
                logger->log_message(NAME, "Not enough time pass to run OSC optimization");
            }
            oscSizeMB = newOptOSCSizeMB;
            elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
            logger->log_message(NAME, "Optimizing DRAM and OSC size took " + std::to_string(elapsedTime) + " seconds");            

            // Wait for OSCCacheUpdateThread
            oscCacheUpdateThread.join();
            logger->log_message(NAME, "Meringue Cache updating is done (at the background)");

            // Update the OSC size and evict accordingly (logical eviction)
            startTime = std::chrono::steady_clock::now();
            long long prvOSCSizeBytes, newOSCSizeBytes;
            runOSCUpdateSizeAndEvict(meringueAddress, newOptOSCSizeMB, prvOSCSizeBytes, newOSCSizeBytes);
            elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
            logger->log_message(NAME, "Updating OSC size and evicting took " + std::to_string(elapsedTime) + " seconds");

            // Reconfigure DRAM Server if necessary
            std::thread dramReconfigThread;
            if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                logger->log_message(NAME, "Reconfigure the DRAM cluster from " + std::to_string(prvDramSizeMB) +
                    " MB to " + std::to_string(newDramSizeMB) + " MB (at the background)");
                dramReconfigThread = std::thread(runDRAMReconfig, prvDramSizeMB, newDramSizeMB, meringueAddress, std::ref(cacheEngineAddresses));
            }

            // Ping Meringue to run OSC eviction with the new OSC size
            startTime = std::chrono::steady_clock::now();
            long long prvTotalSizeBytes, nxtTotalSizeBytes;
            int oscGCPutCount, oscGCGetCount;
            runOSCGC(meringueAddress, prvTotalSizeBytes, nxtTotalSizeBytes, oscGCPutCount, oscGCGetCount);
            elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
            logger->log_message(NAME, "Running OSC garbage collection took " + std::to_string(elapsedTime) + " seconds");
            
            if (CACHE_LEVEL_MODE == TWO_LEVEL) {
                dramReconfigThread.join();
                logger->log_message(NAME, "DRAM reconfiguration is done (at the background)");
            }

            // remove all data in the MBAKERY_ACCESS_LOG_FILE_PATH
            std::filesystem::remove_all(MBAKERY_ACCESS_LOG_FILE_PATH);
            std::filesystem::create_directories(MBAKERY_ACCESS_LOG_FILE_PATH);
            assert(std::filesystem::exists(MBAKERY_ACCESS_LOG_FILE_PATH));

            // log the stats
            long long minute = getCurrentTimeMillis() / 1000LL / 60LL;
            std::ofstream statSummaryFile(MBAKERY_STAT_SUMMARY_FILEPATH, std::ofstream::out | std::ofstream::app);
            for (auto& [cacheEngineAddress, stats] : cacheEngineStats) {
                statSummaryFile << minute << ",CacheEngine," << cacheEngineAddress << "," << std::get<0>(stats) << ",0," << std::get<1>(stats) << "," << std::get<2>(stats) << ",0,0,0,0,0,0\n";
            }
            statSummaryFile << minute << ",MBakery,MBakery,0,0,0,0," << prevOptOSCSizeMB << "," << newOptOSCSizeMB << ",0,0,0,0\n";
            statSummaryFile << minute << ",Meringue,Meringue," << oscGCPutCount << "," << oscGCGetCount << ",0,0,0,0," << prvOSCSizeBytes << "," << newOSCSizeBytes << "," << prvTotalSizeBytes << "," << nxtTotalSizeBytes << "\n";
            statSummaryFile.close();

            auto mainOptimizationElapsedTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - mainOptimizationStartTime).count();
            logger->log_message(NAME, "Main optimization took " + std::to_string(mainOptimizationElapsedTime) + " seconds");
        }
    });
}

void runDRAMReconfig(long long prvDramSizeMB, long long newDramSizeMB, std::string meringueAddress, std::vector<std::string>& cacheEngineAddresses) {
    int prvDRAMServerCount = (prvDramSizeMB + DRAM_UNIT_SIZE_MB - 1) / DRAM_UNIT_SIZE_MB;
    int newDRAMServerCount = (newDramSizeMB + DRAM_UNIT_SIZE_MB - 1) / DRAM_UNIT_SIZE_MB;
    prvDRAMServerCount = std::max(prvDRAMServerCount, 1);
    newDRAMServerCount = std::max(newDRAMServerCount, 1);

    std::string sortFilePath = MERINGUE_SORT_FILE_PATH + "/sorted_osc.bin";
    if (prvDRAMServerCount < newDRAMServerCount) {
        std::thread oscSortAndWriteThread = std::thread(runOSCSortAndWrite, meringueAddress, newDramSizeMB, sortFilePath);

        std::vector<std::string> newCEAddresses, oldCEAddresses;

        auto startTime = std::chrono::steady_clock::now();
        launch_cache_engines(newCEAddresses, newDRAMServerCount - prvDRAMServerCount);
        std::this_thread::sleep_for(std::chrono::seconds(5));
        auto elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Launching cache engines took " + std::to_string(elapsedTimeSec) + " seconds");

        startTime = std::chrono::steady_clock::now();
        start_cache_engines(newCEAddresses);
        std::this_thread::sleep_for(std::chrono::seconds(5));
        elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Starting cache engines took " + std::to_string(elapsedTimeSec) + " seconds");

        startTime = std::chrono::steady_clock::now();
        init_cache_engines(newCEAddresses, meringueAddress);
        std::this_thread::sleep_for(std::chrono::seconds(5));
        elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Initializing cache engines took " + std::to_string(elapsedTimeSec) + " seconds");

        for (std::string address : cacheEngineAddresses) {
            oldCEAddresses.push_back(address);
        }
        for (std::string newCEAddress : newCEAddresses) {
            cacheEngineAddresses.push_back(newCEAddress);
        }

        oscSortAndWriteThread.join();

        logger->log_message(NAME, "Run prefetching new cache engines");
        startTime = std::chrono::steady_clock::now();
        prefetchCacheEngines(newCEAddresses, cacheEngineAddresses, sortFilePath, true);
        elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Prefetching new cache engines took " + std::to_string(elapsedTimeSec) + " seconds");

        std::this_thread::sleep_for(std::chrono::seconds(20));

        startTime = std::chrono::steady_clock::now();
        prefetchCacheEngines(oldCEAddresses, cacheEngineAddresses, sortFilePath, false);
        elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Prefetching old cache engines took " + std::to_string(elapsedTimeSec) + " seconds");
    } else if (prvDRAMServerCount > newDRAMServerCount) {
        std::thread oscSortAndWriteThread = std::thread(runOSCSortAndWrite, meringueAddress, newDramSizeMB, sortFilePath);

        std::vector<std::string> deletedCEAddresses;
        for (int i = 0; i < prvDRAMServerCount - newDRAMServerCount; i++) {
            deletedCEAddresses.push_back(cacheEngineAddresses[i]);
        }
        cacheEngineAddresses.erase(cacheEngineAddresses.begin(), cacheEngineAddresses.begin() + prvDRAMServerCount - newDRAMServerCount);

        std::this_thread::sleep_for(std::chrono::seconds(20));

        auto startTime = std::chrono::steady_clock::now();
        stop_cache_engines(deletedCEAddresses);
        auto elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Stopping cache engines took " + std::to_string(elapsedTimeSec) + " seconds");

        oscSortAndWriteThread.join();

        startTime = std::chrono::steady_clock::now();
        prefetchCacheEngines(cacheEngineAddresses, cacheEngineAddresses, sortFilePath, false);
        elapsedTimeSec = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
        logger->log_message(NAME, "Prefetching cache engines took " + std::to_string(elapsedTimeSec) + " seconds");
    } else {
        logger->log_message(NAME, "No need to reconfigure the DRAM cluster");
        return;
    }

    if (std::filesystem::exists(sortFilePath)) {
        std::filesystem::remove(sortFilePath);
    }
}

void stop_cache_engines(std::vector<std::string>& cacheEngineAddresses) {
    std::vector<std::thread*> threads;
    for (std::string cacheEngineAddress : cacheEngineAddresses) {
        threads.push_back(new std::thread([cacheEngineAddress]() {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(cacheEngineAddress, grpc::InsecureChannelCredentials());
            std::unique_ptr<cacheengine::CacheEngineService::Stub> stub = cacheengine::CacheEngineService::NewStub(channel);
            cacheengine::StopRequest request;
            cacheengine::StopResponse response;
            grpc::ClientContext context;
            grpc::Status status = stub->Stop(&context, request, &response);
            if (!status.ok()) {
                logger->log_message(NAME, "Failed to stop cache engine: " + status.error_message());
            }
        }));
    }
    for (auto thread : threads)
        thread->join();
    for (auto thread : threads)
        delete thread;
    threads.clear();

    std::vector<std::string> deleteInstanceIds;
    for (std::string cacheEngineAddress : cacheEngineAddresses) {
        deleteInstanceIds.push_back(cacheEngineNameToId[cacheEngineAddressToName[cacheEngineAddress]]);
    }
    rmClient->deleteInstances(deleteInstanceIds);
    for (std::string cacheEngineAddress : cacheEngineAddresses) {
        cacheEngineNameToId.erase(cacheEngineAddressToName[cacheEngineAddress]);
        cacheEngineAddressToName.erase(cacheEngineAddress);
    }
}

void prefetchCacheEngines(std::vector<std::string>& ceAddresses, std::vector<std::string>& allCEAddresses, std::string sortFilePath, bool newServer) {
    std::string allCEAddressesString = concatString(allCEAddresses);
    std::vector<std::thread*> threads;
    for (std::string ceAddress : ceAddresses) {
        threads.push_back(new std::thread([ceAddress, allCEAddressesString, newServer, sortFilePath]() {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ceAddress, grpc::InsecureChannelCredentials());
            std::unique_ptr<cacheengine::CacheEngineService::Stub> stub = cacheengine::CacheEngineService::NewStub(channel);
            cacheengine::PrefetchRequest request;
            cacheengine::PrefetchResponse response;
            grpc::ClientContext context;
            request.set_new_server(newServer);
            request.set_target_address(ceAddress);
            request.set_cache_engine_addresses(allCEAddressesString);
            request.set_sort_file_path(sortFilePath);
            grpc::Status status = stub->Prefetch(&context, request, &response);
            if (!status.ok()) {
                logger->log_message(NAME, "Failed to prefetch cache engine: " + status.error_message());
            }
        }));
    }

    for (auto thread : threads)
        thread->join();
    for (auto thread : threads)
        delete thread;
    threads.clear();
}

void runOSCSortAndWrite(std::string meringueAddress, long long newDRAMSizeMB, std::string sortFilePath) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(meringueAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<meringue::MeringueService::Stub> stub = meringue::MeringueService::NewStub(channel);
    meringue::SortAndWriteRequest request;
    meringue::SortAndWriteResponse response;
    grpc::ClientContext context;
    request.set_size_mb(newDRAMSizeMB);
    request.set_filename(sortFilePath);
    grpc::Status status = stub->SortAndWrite(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to sort out OSC: " + status.error_message());
    }
}

void runOSCUpdateCache(std::string meringueAddress, int logFileCount) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(meringueAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<meringue::MeringueService::Stub> stub = meringue::MeringueService::NewStub(channel);
    meringue::UpdateCacheRequest request;
    meringue::UpdateCacheResponse response;
    grpc::ClientContext context;
    request.set_log_file_count(logFileCount);
    grpc::Status status = stub->UpdateCache(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to update cache: " + status.error_message());
    }
}

void runOSCUpdateSizeAndEvict(std::string meringueAddress, long long newOptOSCSizeMB, long long& prvOSCSizeBytes, long long& newOSCSizeBytes) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(meringueAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<meringue::MeringueService::Stub> stub = meringue::MeringueService::NewStub(channel);
    meringue::EvictionRequest request;
    meringue::EvictionResponse response;
    grpc::ClientContext context;
    request.set_new_size_mb(newOptOSCSizeMB);
    grpc::Status status = stub->UpdateSizeAndEvict(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to update OSC size and run eviction: " + status.error_message());
    }
    
    prvOSCSizeBytes = response.prv_osc_size_bytes();
    newOSCSizeBytes = response.new_osc_size_bytes();
}

void runOSCGC(std::string meringueAddress, long long& prvTotalSizeBytes, long long& nxtTotalSizeBytes, int& oscGCPutCount, int& oscGCGetCount) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(meringueAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<meringue::MeringueService::Stub> stub = meringue::MeringueService::NewStub(channel);
    meringue::GCRequest request;
    meringue::GCResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->RunGC(&context, request, &response);
    if (!status.ok()) {
        throw std::runtime_error("Failed to GC: " + status.error_message());
    }

    prvTotalSizeBytes = response.prv_total_data_size_bytes();
    nxtTotalSizeBytes = response.new_total_data_size_bytes();
    oscGCPutCount = response.osc_gc_put_count();
    oscGCGetCount = response.osc_gc_get_count();
}

void runCostMinisimLambda(CloudClient *client, long long minute, int logFileCount, bool contd) {
    std::vector<std::thread> threads;
    for (int i = 1; i <= COST_MINICACHE_COUNT; i++) {
        const int idx = i;
        threads.emplace_back([idx, client, minute, logFileCount, contd]() {
            std::map<std::string, std::pair<std::string, std::string>> mapPayload;
            mapPayload["Minute"] = std::make_pair("long long", std::to_string(minute));
            mapPayload["Contd"] = std::make_pair("bool", contd ? "true" : "false");
            mapPayload["MountPath"] = std::make_pair("string", LAMBDA_MOUNT_PATH);
            mapPayload["ExpName"] = std::make_pair("string", EXP_NAME);
            mapPayload["CacheSizeMB"] = std::make_pair("long long", std::to_string(COST_CACHE_SIZE_UNIT_MB * idx));
            mapPayload["MinisimHashSize"] = std::make_pair("int", std::to_string(MINISIM_HASH_SIZE));
            mapPayload["SamplingRatio"] = std::make_pair("double", std::to_string(COST_SAMPLING_RATIO));
            mapPayload["LogDirPath"] = std::make_pair("string", LAMBDA_ACCESS_LOG_FILE_PATH + "/" + EXP_NAME);
            mapPayload["LogFileCount"] = std::make_pair("int", std::to_string(logFileCount));

            client->InvokeLambda("CostMinisim", mapPayload);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }
}

void runLatencyMinisimLambda(CloudClient *client, long long minute, int logFileCount, bool contd) {
    std::vector<std::thread> threads;
    for (int i = 1; i <= LATENCY_MINICACHE_COUNT; i++) {
        const int idx = i;
        threads.emplace_back([idx, client, minute, logFileCount, contd]() {
            std::map<std::string, std::pair<std::string, std::string>> mapPayload;
            mapPayload["Minute"] = std::make_pair("long long", std::to_string(minute));
            mapPayload["Contd"] = std::make_pair("bool", contd ? "true" : "false");
            mapPayload["MountPath"] = std::make_pair("string", LAMBDA_MOUNT_PATH);
            mapPayload["ExpName"] = std::make_pair("string", EXP_NAME);
            mapPayload["DRAMSizeMB"] = std::make_pair("long long", std::to_string(LATENCY_CACHE_SIZE_UNIT_MB * idx));
            mapPayload["OSCSizeByte"] = std::make_pair("long long", std::to_string(oscSizeMB * 1024LL * 1024LL));
            mapPayload["MinisimHashSize"] = std::make_pair("int", std::to_string(MINISIM_HASH_SIZE));
            mapPayload["SamplingRatio"] = std::make_pair("double", std::to_string(LATENCY_SAMPLING_RATIO));
            mapPayload["LatencyDirPath"] = std::make_pair("string", LAMBDA_MOUNT_PATH);
            mapPayload["LogDirPath"] = std::make_pair("string", LAMBDA_ACCESS_LOG_FILE_PATH + "/" + EXP_NAME);
            mapPayload["RemoteRegion"] = std::make_pair("string", REMOTE_REGION);
            mapPayload["LogFileCount"] = std::make_pair("int", std::to_string(logFileCount));

            client->InvokeLambda("LatencyMinisim", mapPayload);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }
}

long long optimizeOSCSizeMB(int cacheSizeMBs[], double mrc[], long long bmc[], int getCnt, int putCnt, float avgBlockPortion) {
    long long optimalOSCSizeMB = -1L;
    double minCost = 1e9;
    for (int i = 0; i < COST_MINICACHE_COUNT; i++) {
        double cost = computeExpectedCost(cacheSizeMBs[i], mrc[i], bmc[i], getCnt, putCnt, avgBlockPortion);
        optimalOSCSizeMB = minCost > cost ? cacheSizeMBs[i] : optimalOSCSizeMB;
        minCost = minCost > cost ? cost : minCost;
    }
    return optimalOSCSizeMB;
}

double computeExpectedCost(int cacheSizeMB, double mr, long long bm, int getCnt, int putCnt, float avgBlockPortion) {
    double capacityCost = ((double) cacheSizeMB / 1024.) * OS_CAPACITY_PRICE_PER_GB_MIN * CACHE_ENGINE_FLUSH_INTERVAL_SEC / 60. / avgBlockPortion;
    double egressCost = (bm / (double) GB_TO_BYTE) * TRANSFER_PRICE_PER_GB;
    double putOpCost = (getCnt * mr + putCnt) * OS_PUT_PRICE_PER_OP;
    return capacityCost + egressCost + putOpCost;
}

CloudClient *oscClient, *dlClient;
void serve(std::string& meringueAddress, std::vector<std::string>& cacheEngineAddresses) {
    // Start Meringue server
    std::string meringueIpAddress;
    int meringuePort;
    if (!splitIpAddressAndPort(meringueAddress, meringueIpAddress, meringuePort)) {
        throw std::runtime_error("Invalid Meringue address: " + meringueAddress);
    }
    start_meringue_server(meringueIpAddress);
    init_meringue_server(meringueAddress);

    // Create buckets for OSC and DataLake
    oscClient = new CloudClient();
    if (IS_ON_PREM) {
        oscClient->ConnectS3OnPrem(LOCAL_ENDPOINT_URL);
    } else {
        oscClient->ConnectS3Cloud(LOCAL_REGION);
    }
    if (oscClient->DoesBucketExist(OSC_BUCKET_NAME)) {
        logger->log_message(NAME, "OSC Bucket already exists: " + OSC_BUCKET_NAME +". Delete it.");
        oscClient->DeleteBucket(OSC_BUCKET_NAME);
    }
    oscClient->CreateBucket(OSC_BUCKET_NAME);
    logger->log_message(NAME, "Created OSC bucket: " + OSC_BUCKET_NAME);

    dlClient = new CloudClient();
    if (IS_ON_PREM) {
        dlClient->ConnectS3OnPrem(REMOTE_ENDPOINT_URL);
    } else {
        dlClient->ConnectS3Cloud(REMOTE_REGION);
    }
    if (!dlClient->DoesBucketExist(DL_BUCKET_NAME)) {
        dlClient->CreateBucket(DL_BUCKET_NAME);
        logger->log_message(NAME, "Created DataLake bucket: " + DL_BUCKET_NAME);
    } else {
        logger->log_message(NAME, "DataLake bucket already exists: " + DL_BUCKET_NAME);
    }

    // Start cache engines
    if (IS_ON_PREM) {
        assert(CACHE_LEVEL_MODE == SINGLE_LEVEL);
    } else if (CACHE_LEVEL_MODE == TWO_LEVEL) {
        rmClient = new CloudClient();
        rmClient->ConnectEC2(LOCAL_REGION);
        launch_cache_engines(cacheEngineAddresses, 1);
    }
    start_cache_engines(cacheEngineAddresses);
    init_cache_engines(cacheEngineAddresses, meringueAddress);

    // Prepare access log directories
    std::filesystem::remove_all(MBAKERY_ACCESS_LOG_FILE_PATH);
    std::filesystem::create_directories(MBAKERY_ACCESS_LOG_FILE_PATH);

    // Start gRPC server of MBakery
    logger->log_message(NAME, "Start running gRPC server of MBakery");
    grpc::ServerBuilder builder;
    builder.AddListeningPort("[::]:50051", grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);

    MBakeryServiceImpl service(cacheEngineAddresses, meringueAddress);
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    logger->log_message(NAME, "Start listening on [::]:50051");
    server->Wait();
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc("Expected options");
    desc.add_options()
        ("expname", boost::program_options::value<std::string>(), "experiment name")
        ("is_on_prem", boost::program_options::value<bool>()->default_value(false), "environment: onprem or cloud")
        ("warmup_time_min", boost::program_options::value<int>()->default_value(24 * 60), "warming up time for the optimization (minutes)")
        ("optimization_interval_sec", boost::program_options::value<int>()->default_value(15 * 60), "time interval between two consecutive optimization triggers")
        ("cost_minisim_cache_size_unit_mb", boost::program_options::value<int>(), "cache size unit (MB) for the cost miniature simulation")
        ("cost_minisim_cache_count", boost::program_options::value<int>(), "number of mini-caches for the cost miniature simulation")
        ("cache_engine_addresses", boost::program_options::value<std::vector<std::string>>()->multitoken(), "cache engine addresses")
        ("meringue_address", boost::program_options::value<std::string>(), "meringue address")
        ("local_endpoint_url", boost::program_options::value<std::string>(), "local endpoint URL")
        ("remote_endpoint_url", boost::program_options::value<std::string>(), "remote endpoint URL")
        ("local_region", boost::program_options::value<std::string>(), "local region")
        ("remote_region", boost::program_options::value<std::string>(), "remote region")
        ("object_packing_enabled", boost::program_options::value<bool>()->default_value(false), "object packing enabled")
        ("dram_cache_enabled", boost::program_options::value<bool>()->default_value(false), "dram cache is enabled")
        ("dram_vm_type", boost::program_options::value<std::string>(), "dram vm type")
        ("latency_minisim_cache_size_unit_mb", boost::program_options::value<int>(), "cache size unit (MB) for the latency miniature simulation")
        ("latency_minisim_cache_count", boost::program_options::value<int>(), "number of mini-caches for the latency miniature simulation")
        ("latency_logging_enabled", boost::program_options::value<bool>()->default_value(false), "latency logging is enabled")
        ("latency_target", boost::program_options::value<long long>(), "target latency for DRAM cluster");
    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("expname")) {
        EXP_NAME = vm["expname"].as<std::string>();
        MERINGUE_DB_PATH = MERINGUE_DB_PATH + "/" + vm["expname"].as<std::string>();
        COST_DB_BASE_DIR_PATH = COST_DB_BASE_DIR_PATH + "/" + vm["expname"].as<std::string>();
        CACHE_ENGINE_LOG_FILE_PATH = CACHE_ENGINE_LOG_FILE_PATH + "/" + vm["expname"].as<std::string>();
        MBAKERY_ACCESS_LOG_FILE_PATH = MBAKERY_ACCESS_LOG_FILE_PATH + "/" + vm["expname"].as<std::string>();
        OSC_BUCKET_NAME = OSC_BUCKET_NAME + "-" + vm["expname"].as<std::string>();
        DL_BUCKET_NAME = DL_BUCKET_NAME + "-" + vm["expname"].as<std::string>();
        MBAKERY_STAT_SUMMARY_FILEPATH = MBAKERY_STAT_SUMMARY_FILEPATH + "/" + vm["expname"].as<std::string>();
    } else {
        throw std::runtime_error("expname is not provided");
    }

    IS_ON_PREM = vm.count("is_on_prem") ? vm["is_on_prem"].as<bool>() : false;
    std::cout << "* Environment: " << (IS_ON_PREM ? "On Prem" : "Cloud") << std::endl;

    if (vm.count("warmup_time_min")) {
        WARMUP_TIME_MINUTE = vm["warmup_time_min"].as<int>();
    }
    std::cout << "* Warmup time: " << WARMUP_TIME_MINUTE << " minutes" << std::endl;

    if (vm.count("optimization_interval_sec")) {
        CACHE_ENGINE_FLUSH_INTERVAL_SEC = vm["optimization_interval_sec"].as<int>();
    }
    std::cout << "* Optimization interval: " << CACHE_ENGINE_FLUSH_INTERVAL_SEC << " seconds" << std::endl;

    if (vm.count("cost_minisim_cache_size_unit_mb")) {
        COST_CACHE_SIZE_UNIT_MB = vm["cost_minisim_cache_size_unit_mb"].as<int>();
    }
    std::cout << "* cost miniature cache size unit: " << COST_CACHE_SIZE_UNIT_MB << " MB" << std::endl;

    if (vm.count("cost_minisim_cache_count")) {
        COST_MINICACHE_COUNT = vm["cost_minisim_cache_count"].as<int>();
    }
    std::cout << "* cost miniature cache count: " << COST_MINICACHE_COUNT << std::endl;

    logger = new Logger(NAME);

    std::string meringueAddress = vm.count("meringue_address") ? vm["meringue_address"].as<std::string>() : "localhost:50052";

    std::vector<std::string> cacheEngineAddresses = { "localhost:50053" };
    if (vm.count("cache_engine_addresses")) {
        cacheEngineAddresses = vm["cache_engine_addresses"].as<std::vector<std::string>>();
    }

    if (vm.count("local_endpoint_url")) {
        LOCAL_ENDPOINT_URL = vm["local_endpoint_url"].as<std::string>();
    }

    if (vm.count("remote_endpoint_url")) {
        REMOTE_ENDPOINT_URL = vm["remote_endpoint_url"].as<std::string>();
    }

    if (vm.count("local_region")) {
        LOCAL_REGION = vm["local_region"].as<std::string>();
    }

    if (vm.count("remote_region")) {
        REMOTE_REGION = vm["remote_region"].as<std::string>();
        if (!IS_ON_PREM) {
            OSC_BUCKET_NAME = OSC_BUCKET_NAME + "-" + LOCAL_REGION;
            DL_BUCKET_NAME = DL_BUCKET_NAME + "-" + REMOTE_REGION;
        }
    }

    if (vm["object_packing_enabled"].as<bool>()) {
        std::cout << "* Object packing is enabled" << std::endl;
        PACKING_MODE = WITH_PACKING;
    } else {
        std::cout << "* Object packing is disabled" << std::endl;
        PACKING_MODE = NO_PACKING;
    }

    if (vm["dram_cache_enabled"].as<bool>()) {
        std::cout << "* DRAM cache is enabled" << std::endl;
        CACHE_LEVEL_MODE = TWO_LEVEL;
        if (vm.count("dram_vm_type") && vm.count("latency_minisim_cache_size_unit_mb") && vm.count("latency_minisim_cache_count")) {
            CACHE_ENGINE_INSTANCE_TYPE = vm["dram_vm_type"].as<std::string>();
            LATENCY_CACHE_SIZE_UNIT_MB = vm["latency_minisim_cache_size_unit_mb"].as<int>();
            LATENCY_MINICACHE_COUNT = vm["latency_minisim_cache_count"].as<int>();
            LATENCY_TARGET = vm["latency_target"].as<long long>();
            std::cout << "* DRAM VM type: " << CACHE_ENGINE_INSTANCE_TYPE << std::endl;
            std::cout << "* Latency miniature cache size unit: " << LATENCY_CACHE_SIZE_UNIT_MB << " MB" << std::endl;
            std::cout << "* Latency miniature cache count: " << LATENCY_MINICACHE_COUNT << std::endl;
            std::cout << "* Latency target: " << LATENCY_TARGET << " us" << std::endl;
        }
    } else {
        std::cout << "* DRAM cache is disabled" << std::endl;
        CACHE_LEVEL_MODE = SINGLE_LEVEL;
    }

    if (!IS_ON_PREM && CACHE_LEVEL_MODE == TWO_LEVEL)
        cacheEngineAddresses.clear();

    if (vm["latency_logging_enabled"].as<bool>()) {
        std::cout << "* Latency logging is enabled" << std::endl;
        LATENCY_LOGGING_ENABLED = true;
    } else {
        std::cout << "* Latency logging is disabled" << std::endl;
        LATENCY_LOGGING_ENABLED = false;
    }

    serve(meringueAddress, cacheEngineAddresses);

    return 0;
}

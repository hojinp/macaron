#ifndef MBAKERY_HPP
#define MBAKERY_HPP

#include <vector>
#include <grpcpp/grpcpp.h>

#include "cloudapis.hpp"
#include "cost_minisim.hpp"
#include "enums.hpp"
#include "mbakery.grpc.pb.h"
#include "mbakery.pb.h"

class MBakeryServiceImpl final : public mbakery::MBakeryService::Service {
public:
    MBakeryServiceImpl(std::vector<std::string>& cacheEngineAddresses, std::string& meringueAddress);

    grpc::Status Connect(grpc::ServerContext* context, const mbakery::ConnectRequest* request, mbakery::ConnectResponse* response) override;

    grpc::Status OptTrigger(grpc::ServerContext* context, const mbakery::OptTriggerRequest* request, mbakery::OptTriggerResponse* response) override;

    grpc::Status OptStop(grpc::ServerContext* context, const mbakery::OptStopRequest* request, mbakery::OptStopResponse* response) override;

private:
    std::vector<std::string>& cacheEngineAddresses;
    std::string meringueAddress;
    bool optProcessed = false;
    bool optTriggered = false;
};


void start_meringue_server(std::string& meringueIpAddress);
void init_meringue_server(std::string& meringueAddress);

void start_cache_engines(std::vector<std::string>& cacheEngineAddresses);
void init_cache_engines(std::vector<std::string>& cacheEngineAddresses, std::string& meringueAddress);

void start_cache_engine_flush_thread(std::vector<std::string>& cacheEngineAddresses, std::string& meringueAddress);
int computeNewDRAMSizeMB(int dramSizeMB, int scaleInCounter);

void runDRAMReconfig(long long prevDRAMSizeMB, long long newDRAMSizeMB, std::string meringueAddress, std::vector<std::string>& cacheEngineAddresses);
void stop_cache_engines(std::vector<std::string>& cacheEngineAddresses);
void prefetchCacheEngines(std::vector<std::string>& ceAddresses, std::vector<std::string>& allCEAddresses, std::string sortFilePath, bool newServer);
void runOSCUpdateCache(std::string meringueAddress, int logFileCount);
void runOSCUpdateSizeAndEvict(std::string meringueAddress, long long newOptOSCSizeMB, long long& prvOSCSizeBytes, long long& newOSCSizeBytes);
void runOSCGC(std::string meringueAddress, long long& prvTotalSizeBytes, long long& nxtTotalSizeBytes, int& oscGCPutCount, int& oscGCGetCount);
void runOSCSortAndWrite(std::string meringueAddress, long long newDRAMSizeMB, std::string sortFilePath);

void runCostMinisimLambda(CloudClient *client, long long minute, int logFileCount, bool contd);
void runLatencyMinisimLambda(CloudClient *client, long long minute, int logFileCount, bool contd);

long long optimizeDRAMSizeMB(int cacheSizeMBs[], long long elc[], int length, long long latencyTarget);
long long optimizeOSCSizeMB(int cacheSizeMBs[], double mrc[], long long bmc[], int getCnt, int putCnt, float avgBlockPortion);
double computeExpectedCost(int cacheSizeMB, double mr, long long bm, int getCnt, int putCnt, float avgBlockPortion);

void serve(const std::string& meringueAddress, const std::vector<std::string>& cacheEngineAddresses);

#endif // MBAKERY_HPP

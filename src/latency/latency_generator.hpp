#ifndef LATENCY_GENERATOR_HPP
#define LATENCY_GENERATOR_HPP

#include <string>

class LatencyGenerator {
public:
    LatencyGenerator(std::string data_dirpath, std::string remoteRegion);
    long long getMedDRAMLatency(long long size);
    long long getMedOSCLatency(long long size);
    long long getMedDLLatency(long long size);
    long long getAvgDRAMLatency(long long size);
    long long getAvgOSCLatency(long long size);
    long long getAvgDLLatency(long long size);
    
    void parseLatenciesFromFile(std::string filename, long long& med, long long& avg);

private:
    long long* sizes;
    long long* medDRAMLatency;
    long long* medOSCLatency;
    long long* medDLLatency;
    long long* avgDRAMLatency;
    long long* avgOSCLatency;
    long long* avgDLLatency;
    int measuredCount;

    double dramSlope;
    double osSlope;
};

#endif // LATENCY_GENERATOR_HPP
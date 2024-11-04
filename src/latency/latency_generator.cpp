#include <vector>
#include <filesystem>
#include <algorithm>
#include <string>
#include <fstream>
#include <sstream>
#include <cassert>

#include "latency_generator.hpp"

LatencyGenerator::LatencyGenerator(std::string data_dirpath, std::string remoteRegion) {
    std::string latency_dir_path = data_dirpath + "/latency";
    if (remoteRegion == "us-west-1") {
        latency_dir_path += "/use1-usw1";
    } else if (remoteRegion == "eu-central-1") {
        latency_dir_path += "/use1-euc1";
    } else {
        throw new std::runtime_error("Unsupported region: " + remoteRegion);
    }
    latency_dir_path += "/latency_e2e";

    std::vector<std::string> latency_files;
    for (const auto& entry : std::filesystem::directory_iterator(latency_dir_path + "/datalake")) {
        latency_files.push_back(entry.path().filename().string());
    }
    measuredCount = latency_files.size();
    sizes = new long long[measuredCount];
    for (int i = 0; i < measuredCount; i++) {
        sizes[i] = std::stoll(latency_files[i].substr(3, latency_files[i].size() - 6));
    }
    std::sort(sizes, sizes + measuredCount);

    medDRAMLatency = new long long[measuredCount];
    medOSCLatency = new long long[measuredCount];
    medDLLatency = new long long[measuredCount];
    avgDRAMLatency = new long long[measuredCount];
    avgOSCLatency = new long long[measuredCount];
    avgDLLatency = new long long[measuredCount];
    for (int i = 0; i < measuredCount; i++) {
        parseLatenciesFromFile(latency_dir_path + "/dram/get" + std::to_string(sizes[i]) + "B.csv", medDRAMLatency[i], avgDRAMLatency[i]);
        parseLatenciesFromFile(latency_dir_path + "/osc/get" + std::to_string(sizes[i]) + "B.csv", medOSCLatency[i], avgOSCLatency[i]);
        parseLatenciesFromFile(latency_dir_path + "/datalake/get" + std::to_string(sizes[i]) + "B.csv", medDLLatency[i], avgDLLatency[i]);
    }
    
    dramSlope = (double) (avgDRAMLatency[measuredCount - 1] - avgDRAMLatency[measuredCount - 3]) / (double) (sizes[measuredCount - 1] - sizes[measuredCount - 3]);
    double slope1 = (double) (avgOSCLatency[measuredCount - 1] - avgOSCLatency[measuredCount - 3]) / (double) (sizes[measuredCount - 1] - sizes[measuredCount - 3]);
    double slope2 = (double) (avgDLLatency[measuredCount - 1] - avgDLLatency[measuredCount - 3]) / (double) (sizes[measuredCount - 1] - sizes[measuredCount - 3]);
    osSlope = (slope1 + slope2) / 2.0;
};

void LatencyGenerator::parseLatenciesFromFile(std::string filename, long long& med, long long& avg) {
    std::ifstream file(filename);
    std::string line, tmp;
    bool first = true;

    std::vector<long long> values;
    while (std::getline(file, line)) {
        if (first) {
            first = false;
            continue;
        }

        std::stringstream ss(line);
        std::getline(ss, tmp, ',');
        std::getline(ss, tmp);
        values.push_back(std::stoll(tmp));
    }

    std::sort(values.begin(), values.end());
    file.close();

    med = values[values.size() / 2];
    double avgLatency = 0.;
    for (long long lat : values) {
        avgLatency += (double) lat / (double) values.size();
    }
    avg = (long long) avgLatency;
}

long long LatencyGenerator::getMedDRAMLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = medDRAMLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = medDRAMLatency[i], lowLat = medDRAMLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = medDRAMLatency[measuredCount - 1] + (long long) (dramSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}

long long LatencyGenerator::getAvgDRAMLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = avgDRAMLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = avgDRAMLatency[i], lowLat = avgDRAMLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = avgDRAMLatency[measuredCount - 1] + (long long) (dramSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}


long long LatencyGenerator::getMedOSCLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = medOSCLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = medOSCLatency[i], lowLat = medOSCLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = medOSCLatency[measuredCount - 1] + (long long) (osSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}

long long LatencyGenerator::getAvgOSCLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = avgOSCLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = avgOSCLatency[i], lowLat = avgOSCLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = avgOSCLatency[measuredCount - 1] + (long long) (osSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}

long long LatencyGenerator::getMedDLLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = medDLLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = medDLLatency[i], lowLat = medDLLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = medDLLatency[measuredCount - 1] + (long long) (osSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}

long long LatencyGenerator::getAvgDLLatency(long long size) {
    assert(size > 0LL);
    
    long long latency = 0LL;
    if (size == 1LL) {
        assert(sizes[0] == 1LL);
        latency = avgDLLatency[0];
    } else if (size <= sizes[measuredCount - 1]) {
        for (int i = 1; i < measuredCount; i++) {
            if (sizes[i] >= size) {
                long long highSize = sizes[i], lowSize = sizes[i - 1];
                long long highLat = avgDLLatency[i], lowLat = avgDLLatency[i - 1];
                latency = lowLat + ((double) (highLat - lowLat) * ((double) (size - lowSize) / (highSize - lowSize)));
                break;
            }
        }
    } else {
        latency = avgDLLatency[measuredCount - 1] + (long long) (osSlope * (size - sizes[measuredCount - 1]));
    }
    
    assert(latency > 0LL);
    return latency;
}

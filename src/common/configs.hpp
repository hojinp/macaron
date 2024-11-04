#ifndef CONFIGS_HPP
#define CONFIGS_HPP

#include <string>
#include "enums.hpp"

extern std::string NFS_PATH;

extern bool IS_ON_PREM;

extern PackingMode PACKING_MODE;
extern long MAX_BLOCK_SIZE;
extern int MAX_BLOCK_OBJECT_COUNT;

extern std::string MERINGUE_DB_PATH;
extern bool MERINGUE_INMEMORY_MODE;
extern double MERINGUE_GC_THRESHOLD;
extern std::string MERINGUE_SORT_FILE_PATH;

extern std::string LOCAL_ENDPOINT_URL;
extern std::string LOCAL_REGION;
extern std::string OSC_BUCKET_NAME;

extern std::string REMOTE_ENDPOINT_URL;
extern std::string REMOTE_REGION;
extern std::string DL_BUCKET_NAME;

extern CloudServiceProvider LOCAL_CSP;
extern CloudServiceProvider DL_CSP;

extern CacheLevelMode CACHE_LEVEL_MODE;

extern std::string CACHE_ENGINE_LOG_FILE_PATH;
extern int CACHE_ENGINE_FLUSH_INTERVAL_SEC;

// MBakery log file paths
extern std::string MBAKERY_ACCESS_LOG_FILE_PATH;
extern std::string MBAKERY_STAT_SUMMARY_FILEPATH;

// Lambda log file path
extern std::string LAMBDA_ACCESS_LOG_FILE_PATH;

// Optimization parameter
extern long long INITIAL_OSC_SIZE_MB;
extern long long INITIAL_DRAM_SIZE_MB;
extern long long DRAM_UNIT_SIZE_MB;
extern int WARMUP_TIME_MINUTE;

// Consistent hashing
extern int CONSISTENT_HASH_SIZE;

// CostMiniatureSimulation
extern int MINISIM_HASH_SIZE;
extern double COST_SAMPLING_RATIO;
extern int COST_MINICACHE_COUNT;
extern int COST_CACHE_SIZE_UNIT_MB;
extern std::string COST_DB_BASE_DIR_PATH;

// LatencyMiniatureSimulation
extern int LATENCY_MIN_REQ_COUNT;
extern int DRAM_SCALE_IN_THRESHOLD;
extern double LATENCY_SAMPLING_RATIO;
extern int LATENCY_MINICACHE_COUNT;
extern int LATENCY_CACHE_SIZE_UNIT_MB;
extern long long LATENCY_TARGET;
extern std::string LATENCY_DB_BASE_DIR_PATH;

// AWS Lambda
extern bool USE_LAMBDA;
extern std::string LAMBDA_MOUNT_PATH;

// Cost parameters
extern double OS_CAPACITY_PRICE_PER_GB_MIN;
extern double OS_PUT_PRICE_PER_OP;
extern double TRANSFER_PRICE_PER_GB;

// Constant values
extern long long KB_TO_BYTE;
extern long long MB_TO_BYTE;
extern long long GB_TO_BYTE;

// Macaron Client
extern std::string MACARON_CLIENT_DEBUG_FILE_PATH;
extern std::string MACARON_CLIENT_LATENCY_LOG_FILE_PATH;

// Latency logging
extern bool LATENCY_LOGGING_ENABLED;

extern std::string CACHE_ENGINE_IMG_ID;
extern std::string CACHE_ENGINE_INSTANCE_TYPE;
extern std::string KEY_PAIR_NAME;
extern std::string VPC_ID;
extern std::string SUBNET_ID;
extern std::string SECURITY_GROUP_ID;

#endif // CONFIGS_HPP
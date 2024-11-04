#include "configs.hpp"
#include "enums.hpp"
#include <string>
#include <cstdlib>


std::string HOME_DIR = getenv("HOME");
std::string LOCAL_DISK_PATH = HOME_DIR + "/mnt/localDisk";
std::string NFS_PATH = HOME_DIR + "/mnt/efs0";

bool IS_ON_PREM = false;

PackingMode PACKING_MODE = NO_PACKING;
long MAX_BLOCK_SIZE = 16 * 1024 * 1024;
int MAX_BLOCK_OBJECT_COUNT = 40;

std::string MERINGUE_DB_PATH = LOCAL_DISK_PATH + "/meringue";
bool MERINGUE_INMEMORY_MODE = true;
double MERINGUE_GC_THRESHOLD = 0.5;
std::string MERINGUE_SORT_FILE_PATH = NFS_PATH + "/tmpdir/meringue";

std::string LOCAL_ENDPOINT_URL = "http://localhost:9000";
std::string LOCAL_REGION = "us-east-1";
std::string OSC_BUCKET_NAME = "osc-bucket";

std::string REMOTE_ENDPOINT_URL = "http://localhost:9000";
std::string REMOTE_REGION = "us-west-1";
std::string DL_BUCKET_NAME = "dl-bucket";

CloudServiceProvider LOCAL_CSP = CloudServiceProvider::AWS;
CloudServiceProvider DL_CSP = CloudServiceProvider::AWS;

CacheLevelMode CACHE_LEVEL_MODE = CacheLevelMode::SINGLE_LEVEL;

std::string CACHE_ENGINE_LOG_FILE_PATH = NFS_PATH + "/tmpdir/cache_engine";
int CACHE_ENGINE_FLUSH_INTERVAL_SEC = 60;

std::string MBAKERY_ACCESS_LOG_FILE_PATH = NFS_PATH + "/tmpdir/mbakery";
std::string MBAKERY_STAT_SUMMARY_FILEPATH = LOCAL_DISK_PATH + "/mbakery_stats";

bool USE_LAMBDA = true;
std::string LAMBDA_MOUNT_PATH = "/mnt/efs0";
std::string LAMBDA_ACCESS_LOG_FILE_PATH = LAMBDA_MOUNT_PATH + "/tmpdir/mbakery";

long long INITIAL_OSC_SIZE_MB = 100LL * 1024LL * 1024LL;
long long INITIAL_DRAM_SIZE_MB = 26LL * 1024LL;
long long DRAM_UNIT_SIZE_MB = 26LL * 1024LL;
int WARMUP_TIME_MINUTE = 24 * 60; // 24 hours

int CONSISTENT_HASH_SIZE = 100000000;

int MINISIM_HASH_SIZE = 100000000;

double COST_SAMPLING_RATIO = 0.05;
int COST_MINICACHE_COUNT = 200;
int COST_CACHE_SIZE_UNIT_MB = 100;
std::string COST_DB_BASE_DIR_PATH = LOCAL_DISK_PATH + "/cost_minisim";

int LATENCY_MIN_REQ_COUNT = 500;
int DRAM_SCALE_IN_THRESHOLD = 4;
double LATENCY_SAMPLING_RATIO = 0.05;
int LATENCY_MINICACHE_COUNT = 200;
int LATENCY_CACHE_SIZE_UNIT_MB = 100;
long long LATENCY_TARGET = 100000; // 100ms
std::string LATENCY_DB_BASE_DIR_PATH = LOCAL_DISK_PATH + "/latency_minisim";

double OS_CAPACITY_PRICE_PER_GB_MIN = 0.023 / 30.5 / 24. / 60.;
double OS_PUT_PRICE_PER_OP = 5e-6;
double TRANSFER_PRICE_PER_GB = 0.02;

long long KB_TO_BYTE = 1024LL;
long long MB_TO_BYTE = 1024LL * 1024LL;
long long GB_TO_BYTE = 1024LL * 1024LL * 1024LL;

std::string MACARON_CLIENT_DEBUG_FILE_PATH = LOCAL_DISK_PATH + "/debug";
std::string MACARON_CLIENT_LATENCY_LOG_FILE_PATH = LOCAL_DISK_PATH + "/latency";

bool LATENCY_LOGGING_ENABLED = false;

std::string CACHE_ENGINE_IMG_ID = ""; // Check the repository for the image ID
std::string CACHE_ENGINE_INSTANCE_TYPE = "r5.xlarge";
std::string KEY_PAIR_NAME = ""; // Check the repository for the key pair name
std::string VPC_ID = ""; // Check the repository for the VPC ID
std::string SUBNET_ID = ""; // Check the repository for the subnet ID
std::string SECURITY_GROUP_ID = ""; // Check the repository for the security group ID


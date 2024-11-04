#ifndef UTILS_HPP
#define UTILS_HPP

#include <iostream>
#include <fstream>
#include <sstream>
#include <ctime>
#include <string>
#include <vector>
#include <regex>
#include <iomanip>

#include <crypto++/sha.h>
#include <crypto++/hex.h>

extern bool ENABLE_FILE_LOGGING;
extern const std::string FILE_LOGGING_PATH;
extern const std::string MACARON_BIN_DIR_PATH;
extern const std::string MACARON_CONF_DIR_PATH;
extern const std::string MACARON_DATA_DIR_PATH;
extern const std::string REMOTE_BIN_DIR_PATH;
extern const std::string REMOTE_CONF_DIR_PATH;

class Logger {
public:
    Logger(std::string& name);
    void log_message(const std::string& name, const std::string& message);

private:
    std::string& name;
};

bool send_file(const std::string& localFilePath, const std::string& remoteDirPath, const std::string& remoteAddress);
bool send_file(const std::string& localFilePath, const std::string& remoteDirPath, const std::string& remoteFileName, const std::string& remoteAddress);
void writeLineToFile(std::string& filePath, const std::string& line, bool append);
bool readLastLineFromFile(std::string& filePath, std::string& line);
bool splitIpAddressAndPort(const std::string& input, std::string& ipAddress, int& port);
void splitString(const std::string& input, std::vector<std::string>& splits);
std::string concatString(std::vector<std::string>& splits);
std::string urlDecode(const std::string& str);
std::string formatIpAddress(const std::string& ipAddress);
long long getCurrentTimeMillis();
std::string byteArrayToHexString(const uint8_t* array, size_t size);
int hashId(std::string& key, int hashSize);

#endif // UTILS_HPP
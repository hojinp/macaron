#include "utils.hpp"

bool ENABLE_FILE_LOGGING = true;
const std::string FILE_LOGGING_PATH = "/tmp/macaron";
const std::string MACARON_BIN_DIR_PATH = getenv("HOME") + std::string("/.cmacaron/bin");
const std::string MACARON_CONF_DIR_PATH = getenv("HOME") + std::string("/.cmacaron/conf");
const std::string MACARON_DATA_DIR_PATH = getenv("HOME") + std::string("/.cmacaron/data");
const std::string REMOTE_BIN_DIR_PATH = "/tmp/macaron/bin";
const std::string REMOTE_CONF_DIR_PATH = "/tmp/macaron/conf";

Logger::Logger(std::string& name) : name(name) {
    if (ENABLE_FILE_LOGGING) {
        std::string command = "mkdir -p " + FILE_LOGGING_PATH;
        if (std::system(command.c_str()) != 0) {
            std::cout << "Failed to create directory: " << FILE_LOGGING_PATH << std::endl;
        }

        std::string logFilePath = FILE_LOGGING_PATH + "/" + name + ".log";
        std::ofstream logFile(logFilePath, std::ofstream::out | std::ofstream::app);
        if (!logFile.is_open()) {
            throw std::runtime_error("Failed to open log file: " + logFilePath);
        }
        logFile.close();
    }
}

void Logger::log_message(const std::string& name, const std::string& message) {
    if (ENABLE_FILE_LOGGING) {
        std::time_t now = std::time(nullptr);
        char timestamp[20];
        std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", std::localtime(&now));

        std::string logFilePath = FILE_LOGGING_PATH + "/" + name + ".log";
        std::ofstream logFile(logFilePath, std::ofstream::out | std::ofstream::app);
        if (logFile.is_open()) {
            logFile << "[" << timestamp << "] [" << name << "] " << message << std::endl;
        } else {
            throw std::runtime_error("Failed to open log file: " + logFilePath);
        }
        logFile.close();
    }
}

// Send a file to a remote address using scp
bool send_file(const std::string& localFilePath, const std::string& remoteDirPath, const std::string& remoteAddress) {    
    std::string command = "ssh -o StrictHostKeyChecking=no " + remoteAddress + " 'mkdir -p " + remoteDirPath + "'";
    int result = std::system(command.c_str());
    if (result != 0) {
        std::cerr << "Failed to create remote directory: " << remoteDirPath << std::endl;
        return false;
    }

    command = "scp -o StrictHostKeyChecking=no " + localFilePath + " " + remoteAddress + ":" + remoteDirPath;
    result = std::system(command.c_str());
    if (result == 0) {
        return true;
    } else {
        std::cerr << "Failed to send file: " << localFilePath << " to " << remoteDirPath << std::endl;
        return false;
    }
}

bool send_file(const std::string& localFilePath, const std::string& remoteDirPath, const std::string& remoteFileName, const std::string& remoteAddress) {    
    std::string command = "ssh -o StrictHostKeyChecking=no " + remoteAddress + " 'mkdir -p " + remoteDirPath + "'";
    int result = std::system(command.c_str());
    if (result != 0) {
        std::cerr << "Failed to create remote directory: " << remoteDirPath << std::endl;
        return false;
    }

    command = "scp -o StrictHostKeyChecking=no " + localFilePath + " " + remoteAddress + ":" + remoteDirPath + "/" + remoteFileName;
    result = std::system(command.c_str());
    if (result == 0) {
        return true;
    } else {
        std::cerr << "Failed to send file: " << localFilePath << " to " << remoteDirPath << "/" << remoteFileName << std::endl;
        return false;
    }
}

void writeLineToFile(std::string& filePath, const std::string& line, bool append) {
    std::ofstream file;
    if (append) {
        file.open(filePath, std::ios_base::app);
    } else {
        file.open(filePath);
    }

    if (file.is_open()) {
        file << line << std::endl;
    } else {
        throw std::runtime_error("Failed to open file: " + filePath);
    }

    file.close();
}

bool readLastLineFromFile(std::string& filePath, std::string& line) {
    std::ifstream reqFile(filePath, std::ios_base::in);
    if (reqFile.is_open()) {
        std::string tmp;
        while (std::getline(reqFile, tmp)) {
            line = tmp;
        }
        return true;
    } else {
        return false;
    }
}

bool splitIpAddressAndPort(const std::string& input, std::string& ipAddress, int& port) {
    // Find the position of the colon
    size_t colonPos = input.find(':');

    // Check if the input string contains a colon
    if (colonPos == std::string::npos) {
        return false; // Invalid format
    }

    // Extract the IP address and port
    ipAddress = input.substr(0, colonPos);
    std::string portStr = input.substr(colonPos + 1);

    // Convert the port string to an integer
    std::istringstream portStream(portStr);
    if (!(portStream >> port)) {
        return false; // Invalid port
    }

    return true;
}

void splitString(const std::string& input, std::vector<std::string>& splits) {
    std::istringstream ss(input);
    std::string token;
    while (std::getline(ss, token, ',')) {
        splits.push_back(token);
    }
}

std::string concatString(std::vector<std::string>& splits) {
    std::string output = "";
    for (size_t i = 0; i < splits.size(); ++i) {
        output += splits[i];
        if (i < splits.size() - 1) {
            output += ",";
        }
    }
    return output;
}

std::string urlDecode(const std::string& str) {
    std::stringstream decoded;
    for (size_t i = 0; i < str.length(); ++i) {
        if (str[i] == '%' && i + 2 < str.length()) {
            int hex;
            std::istringstream hexStream(str.substr(i + 1, 2));
            if (hexStream >> std::hex >> hex) {
                decoded << static_cast<char>(hex);
                i += 2;
            } else {
                decoded << str[i];
            }
        } else {
            decoded << str[i];
        }
    }
    return decoded.str();
}

std::string formatIpAddress(const std::string& ipAddress) {
    std::string urlDecoded = urlDecode(ipAddress);
    std::size_t openBracketPos = urlDecoded.find("[");
    std::size_t closeBracketPos = urlDecoded.find("]");
    if (openBracketPos != std::string::npos && closeBracketPos != std::string::npos) {
        std::string ipv6Address = urlDecoded.substr(openBracketPos + 1, closeBracketPos - openBracketPos - 1);
        std::size_t colonPos = urlDecoded.find(":", closeBracketPos);
        if (colonPos != std::string::npos) {
            std::string port = urlDecoded.substr(colonPos + 1);
            return ipv6Address + ":" + port;
        }
    }
    return ipAddress;
}

long long getCurrentTimeMillis() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}


std::string byteArrayToHexString(const uint8_t* array, size_t size) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i) {
        ss << std::setw(2) << static_cast<unsigned int>(array[i]);
    }
    return ss.str();
}


int hashId(std::string& key, int hashSize) {
    CryptoPP::SHA1 sha1;
    uint8_t digest[CryptoPP::SHA1::DIGESTSIZE];
    sha1.CalculateDigest(digest, reinterpret_cast<const byte*>(key.c_str()), key.length());
    std::string digested = byteArrayToHexString(digest, CryptoPP::SHA1::DIGESTSIZE);
    digested = digested.substr(0, 15);
    int hashed = std::stoll(digested, nullptr, 16) % hashSize;
    return hashed;
}


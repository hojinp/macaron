#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>

int main(int argc, char** argv) {
    boost::program_options::options_description desc("Workload parsing options");
    desc.add_options()
        ("tracepath", boost::program_options::value<std::string>(), "Path to the trace file")
        ("outputdirname", boost::program_options::value<std::string>(), "Path to the output file")
        ("maxkey", boost::program_options::value<int>(), "Maximum key value");
    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("tracepath") == 0 || vm.count("outputdirname") == 0) {
        std::cerr << "Usage: workload_analyzer --tracepath=<tracepath> --outputdirname=<outputdirname>" << std::endl;
        return 1;
    }

    std::string tracepath = vm["tracepath"].as<std::string>();
    std::string outputdirname = vm["outputdirname"].as<std::string>();
    long long maxKey = vm["maxkey"].as<int>();

    std::cout << "Trace path: " << tracepath << std::endl;
    std::cout << "Output directory: " << outputdirname << std::endl;
    if (std::filesystem::exists(outputdirname)) {
        std::cerr << "Output directory already exists" << std::endl;
        return 1;
    }
    std::filesystem::create_directories(outputdirname);

    std::string obj_size_filename = outputdirname + "/object_size.csv";
    std::string obj_access_filename = outputdirname + "/object_access.csv";
    std::string obj_first_two_access_filename = outputdirname + "/object_first_two_access.csv";
    long long profileInterval = 15LL * 60LL * 1000LL; // 15 minutes
    long long nextProfileTime = profileInterval;

    // read the trace file line by line
    std::ifstream traceFile(tracepath);
    std::ofstream objsizefile(obj_size_filename, std::ofstream::out | std::ofstream::trunc);
    std::ofstream objaccessfile(obj_access_filename, std::ofstream::out | std::ofstream::trunc);
    std::ofstream objfirsttwoaccessfile(obj_first_two_access_filename, std::ofstream::out | std::ofstream::trunc);
    std::string line;

    objsizefile << "minute,request_count,object_size" << std::endl;
    objaccessfile << "minute,<50ms,<100ms,<200ms,<300ms,<400ms,<500ms,<600ms,<700ms,<800ms,<900ms,<1000ms,>1000ms" << std::endl;
    objfirsttwoaccessfile << "<50ms,<100ms,<200ms,<300ms,<400ms,<500ms,<600ms,<700ms,<800ms,<900ms,<1000ms,>1000ms" << std::endl;

    // 50ms, 100ms, 200ms, 300ms, 400ms, 500ms, 600ms, 700ms, 800ms, 900ms, 1000ms
    int ranges[12], firstTwoRanges[12];
    std::fill_n(ranges, 12, 0);
    std::fill_n(firstTwoRanges, 12, 0);
    int request_count = 0;
    long long avg_object_size = 0LL;
    long long *lastAccessed = new long long[maxKey];
    bool *checkedFirstTwo = new bool[maxKey];
    std::fill_n(lastAccessed, maxKey, -1LL);
    std::fill_n(checkedFirstTwo, maxKey, false);

    while (std::getline(traceFile, line)) {
        if (line[0] == '#') {
            continue;
        }

        long long timestamp;
        int opType;
        int key;
        long long size;
        std::istringstream iss(line);
        iss >> timestamp;
        iss.ignore(1);
        iss >> opType;
        iss.ignore(1);
        iss >> key;
        if (opType != 2) {
            iss.ignore(1);
            iss >> size;
        } else {
            size = 0LL;
        }

        while (timestamp > nextProfileTime) {
            int minute = nextProfileTime / 60000LL;

            if (request_count > 0)
                avg_object_size /= request_count;
            else
                avg_object_size = 0LL;
            objsizefile << minute << "," << request_count << "," << avg_object_size << std::endl;
            request_count = 0;
            avg_object_size = 0LL;

            objaccessfile << minute;
            for (int i = 0; i < 12; i++) {
                objaccessfile << "," << ranges[i];
                ranges[i] = 0;
            }
            objaccessfile << std::endl;

            nextProfileTime += profileInterval;
        }

        if (opType != 2) {
            long long lastAccessedTime = lastAccessed[key];
            lastAccessed[key] = timestamp;
            if (lastAccessedTime != -1LL) {
                long long timeInterval = timestamp - lastAccessedTime;
                if (timeInterval < 50) {
                    ranges[0]++;
                } else if (timeInterval >= 1000) {
                    ranges[11]++;
                } else {
                    ranges[timeInterval / 100 + 1]++;
                }

                if (!checkedFirstTwo[key]) {
                    if (timeInterval < 50) {
                        firstTwoRanges[0]++;
                    } else if (timeInterval >= 1000) {
                        firstTwoRanges[11]++;
                    } else {
                        firstTwoRanges[timeInterval / 100 + 1]++;
                    }
                    checkedFirstTwo[key] = true;
                }
            }

            if (opType == 1) { // GET
                request_count++;
                avg_object_size += size;
            }
        }
    }
    objfirsttwoaccessfile << firstTwoRanges[0];
    for (int i = 1; i < 12; i++) {
        objfirsttwoaccessfile << "," << firstTwoRanges[i];
    }
    objfirsttwoaccessfile << std::endl;

    objsizefile.close();
    objaccessfile.close();
    objfirsttwoaccessfile.close();
    traceFile.close();
    delete[] lastAccessed;
    return 0;
}
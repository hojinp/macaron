#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>

int main(int argc, char** argv) {
    boost::program_options::options_description desc("Workload parsing options");
    desc.add_options()
        ("tracepath", boost::program_options::value<std::string>(), "Path to the trace file")
        ("outputfile", boost::program_options::value<std::string>(), "Path to the output file");
    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("tracepath") == 0 || vm.count("outputfile") == 0) {
        std::cerr << "Usage: parsing_trace --tracepath=<tracepath> --outputfile=<outputfile>" << std::endl;
        return 1;
    }

    std::string tracepath = vm["tracepath"].as<std::string>();
    std::string outputfile = vm["outputfile"].as<std::string>();

    std::cout << "Trace path: " << tracepath << std::endl;
    std::cout << "Output file: " << outputfile << std::endl;
    if (std::filesystem::exists(outputfile)) {
        std::cerr << "Output file already exists" << std::endl;
        return 1;
    }

    // read the trace file line by line
    std::ifstream traceFile(tracepath);
    std::ofstream outputFile(outputfile, std::ofstream::out | std::ofstream::trunc | std::ofstream::binary);
    std::string line;
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

        // then, write the parsed values to the output file in the binary format
        outputFile.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
        outputFile.write(reinterpret_cast<const char*>(&opType), sizeof(opType));
        outputFile.write(reinterpret_cast<const char*>(&key), sizeof(key));
        outputFile.write(reinterpret_cast<const char*>(&size), sizeof(size));
    }
    outputFile.close();
}
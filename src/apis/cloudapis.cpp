#include "cloudapis.hpp"
#include <iostream>
#include <cstdlib>
#include <stdlib.h>
#include <configs.hpp>
#include <fstream>
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>
#include <aws/ec2/EC2Client.h>
#include <aws/ec2/model/RunInstancesRequest.h>
#include <aws/ec2/model/RunInstancesResponse.h>
#include <aws/ec2/model/InstanceType.h>
#include <aws/ec2/model/Instance.h>
#include <aws/ec2/model/TerminateInstancesRequest.h>
#include <aws/ec2/model/TerminateInstancesResponse.h>
#include <cassert>


bool CloudClient::apiInitialized = false;

CloudClient::CloudClient() {
    if (!apiInitialized) {
        Aws::SDKOptions options;
        Aws::InitAPI(options);
        apiInitialized = true;
    }
    testBucketName = "";
}

void CloudClient::ConnectS3OnPrem(const std::string& endpointUrl) {
    std::cout << "Connecting to S3" << std::endl;
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.endpointOverride = Aws::String(endpointUrl);
    clientConfig.scheme = Aws::Http::Scheme::HTTP;

    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(std::getenv("AWS_ACCESS_KEY_ID"));
    credentials.SetAWSSecretKey(std::getenv("AWS_SECRET_ACCESS_KEY"));
    
    nS3Clients = 64;
    s3ClientIndex = 0;
    s3Clients = new std::vector<Aws::S3::S3Client*>();
    for (int i = 0; i < nS3Clients; i++) {
        s3Clients->push_back(new Aws::S3::S3Client(credentials, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false));
    }
}

void CloudClient::setTestBucket(std::string bucketName) {
    testBucketName = bucketName;
}

void CloudClient::runTest(Aws::S3::S3Client* s3Client) {
    assert(testBucketName.size() > 0);
    std::string key = "test";
    std::string data = "a";

    Aws::S3::Model::PutObjectRequest putObjectRequest;
    putObjectRequest.SetBucket(testBucketName.c_str());
    putObjectRequest.SetKey(key.c_str());
    putObjectRequest.SetContentType("text/plain");
    auto dataStream = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    *dataStream << data;
    putObjectRequest.SetBody(dataStream);
    auto putOutcome = s3Client->PutObject(putObjectRequest);

    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket(testBucketName.c_str());
    getObjectRequest.SetKey(key.c_str());
    auto getObjectOutcome = s3Client->GetObject(getObjectRequest);
    data = Aws::Utils::StringUtils::to_string(getObjectOutcome.GetResult().GetBody().rdbuf());
}

void CloudClient::ConnectS3Cloud(const std::string& region, bool proxy) {
    std::cout << "Connecting to S3" << std::endl;
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::String(region);
    this->region = region;

    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(std::getenv("AWS_ACCESS_KEY_ID"));
    credentials.SetAWSSecretKey(std::getenv("AWS_SECRET_ACCESS_KEY"));

    if (proxy) {
        clientConfig.scheme = Aws::Http::Scheme::HTTP;
        clientConfig.proxyScheme = Aws::Http::Scheme::HTTP;
        clientConfig.proxyHost = "proxy.pdl.cmu.edu";
        clientConfig.proxyPort = 3128;
    }

    nS3Clients = 32;
    s3ClientIndex = 0;
    s3Clients = new std::vector<Aws::S3::S3Client*>();
    for (int i = 0; i < nS3Clients; i++) {
        s3Clients->push_back(new Aws::S3::S3Client(credentials, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false));
    }

    if (testBucketName.size() > 0) {
        std::vector<std::thread> threads;
        for (int i = 0; i < nS3Clients; i++) {
            threads.push_back(std::thread(&CloudClient::runTest, this, s3Clients->at(i)));
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();
    }

    s3ClientRenewThread = std::thread(&CloudClient::RenewS3Clients, this, proxy);
    s3ClientRenewThread.detach();
}

void CloudClient::RenewS3Clients(bool proxy) {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::String(region);

    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(std::getenv("AWS_ACCESS_KEY_ID"));
    credentials.SetAWSSecretKey(std::getenv("AWS_SECRET_ACCESS_KEY"));

    if (proxy) {
        clientConfig.scheme = Aws::Http::Scheme::HTTP;
        clientConfig.proxyScheme = Aws::Http::Scheme::HTTP;
        clientConfig.proxyHost = "proxy.pdl.cmu.edu";
        clientConfig.proxyPort = 3128;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (true) {
        std::vector<Aws::S3::S3Client*> *newS3Clients = new std::vector<Aws::S3::S3Client*>(), *oldS3Clients;
        for (int i = 0; i < nS3Clients; i++) {
            newS3Clients->push_back(new Aws::S3::S3Client(credentials, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false));
        }
        if (testBucketName.size() > 0) {
            std::vector<std::thread> threads;
            for (int i = 0; i < nS3Clients; i++) {
                threads.push_back(std::thread(&CloudClient::runTest, this, s3Clients->at(i)));
            }
            for (auto& thread : threads) {
                thread.join();
            }
            threads.clear();
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
        {
            std::lock_guard<std::mutex> lock(s3ClientMtx);
            oldS3Clients = s3Clients;
            s3Clients = newS3Clients;
        }

        std::thread cleanupThread([this, oldS3Clients]() {
            std::this_thread::sleep_for(std::chrono::seconds(20));
            for (int i = 0; i < nS3Clients; i++) {
                delete oldS3Clients->at(i);
            }
            delete oldS3Clients;
        });
        cleanupThread.detach();
    }
}


bool CloudClient::DoesBucketExist(const std::string& bucket_name) {
    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    try {
        std::cout << "Checking if bucket exists: " << bucket_name << " in region: " << region << std::endl;
        Aws::S3::Model::HeadBucketRequest headBucketRequest;
        headBucketRequest.SetBucket(Aws::String(bucket_name.c_str()));
        auto outcome = s3Client->HeadBucket(headBucketRequest);
        return outcome.IsSuccess();
    } catch (const Aws::S3::S3Error& e) {
        return false;
    }
}

void CloudClient::CreateBucket(const std::string& bucket_name) {
    std::cout << "Creating bucket: " << bucket_name << std::endl;
    Aws::S3::Model::CreateBucketRequest createBucketRequest;
    createBucketRequest.SetBucket(bucket_name.c_str());
    if (region != "ONPREM" && region != "us-east-1") { // us-east-1 is the default region and does not need to be specified
        Aws::S3::Model::CreateBucketConfiguration createBucketConfiguration;
        createBucketConfiguration.SetLocationConstraint(GetBucketLocationConstraint(region));
        createBucketRequest.SetCreateBucketConfiguration(std::move(createBucketConfiguration));
        std::cout << "Set creating bucket configuration for region: " << region << std::endl;
    }

    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    auto outcome = s3Client->CreateBucket(createBucketRequest);
    if (!outcome.IsSuccess()) {
        std::cout << "Error creating bucket: " << outcome.GetError().GetMessage() << std::endl;
        throw std::runtime_error("Error creating bucket.");
    }
}

void CloudClient::ListBucket(const std::string& bucket_name, std::vector<std::string>& keys) {
    Aws::S3::Model::ListObjectsV2Request listObjectsRequest;
    listObjectsRequest.SetBucket(bucket_name.c_str());

    bool done = false;
    while (!done) {
        Aws::S3::S3Client* s3Client;
        {
            std::lock_guard<std::mutex> lock(s3ClientMtx);
            s3Client = s3Clients->at(s3ClientIndex);
            s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
        }

        auto listObjectsOutcome = s3Client->ListObjectsV2(listObjectsRequest);
        if (listObjectsOutcome.IsSuccess()) {
            const Aws::Vector<Aws::S3::Model::Object>& objects = listObjectsOutcome.GetResult().GetContents();
            for (const auto& object : objects) {
                keys.push_back(object.GetKey());
            }

            if (!listObjectsOutcome.GetResult().GetNextContinuationToken().empty()) {
                listObjectsRequest.SetContinuationToken(listObjectsOutcome.GetResult().GetNextContinuationToken());
            } else {
                done = true;
            }
        } else {
            throw std::runtime_error("Error listing objects in the bucket.");
        }
    }
}

Aws::S3::Model::BucketLocationConstraint CloudClient::GetBucketLocationConstraint(const std::string& region) {
    if (region == "us-east-1") {
        return Aws::S3::Model::BucketLocationConstraint::us_east_1;
    } else if (region == "us-west-1") {
        return Aws::S3::Model::BucketLocationConstraint::us_west_1;
    } else if (region == "us-west-2") {
        return Aws::S3::Model::BucketLocationConstraint::us_west_2;
    } else if (region == "eu-central-1") {
        return Aws::S3::Model::BucketLocationConstraint::eu_central_1;
    } else {
        throw std::runtime_error("Unknown region: " + region);
    }
}

void CloudClient::DeleteBucket(const std::string& bucket_name) {
    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    // Get all objects in the bucket
    Aws::S3::Model::ListObjectsV2Request listObjectsRequest;
    listObjectsRequest.SetBucket(bucket_name.c_str());
    auto listObjectsOutcome = s3Client->ListObjectsV2(listObjectsRequest);

    if (listObjectsOutcome.IsSuccess()) {
        const Aws::Vector<Aws::S3::Model::Object>& objects = listObjectsOutcome.GetResult().GetContents();

        if (!objects.empty()) {
            // Delete all objects in the bucket
            Aws::S3::Model::DeleteObjectsRequest deleteObjectsRequest;
            Aws::Vector<Aws::S3::Model::ObjectIdentifier> objectsToDelete;

            for (const auto& object : objects) {
                Aws::S3::Model::ObjectIdentifier identifier;
                identifier.SetKey(object.GetKey());
                objectsToDelete.push_back(identifier);
            }

            Aws::S3::Model::Delete deleteObjects;
            deleteObjects.SetObjects(objectsToDelete);

            deleteObjectsRequest.SetBucket(bucket_name.c_str());
            deleteObjectsRequest.SetDelete(deleteObjects);

            auto deleteOutcome = s3Client->DeleteObjects(deleteObjectsRequest);
            if (!deleteOutcome.IsSuccess()) {
                throw std::runtime_error("Error deleting objects from the bucket.");
            }
        }
    }
    
    Aws::S3::Model::DeleteBucketRequest deleteBucketRequest;
    deleteBucketRequest.SetBucket(bucket_name.c_str());
    auto deleteBucketOutcome = s3Client->DeleteBucket(deleteBucketRequest);
    if (!deleteBucketOutcome.IsSuccess()) {
        throw std::runtime_error("Error deleting the bucket.");
    }
}

void CloudClient::PutData(const std::string& bucket_name, const std::string& key, const std::string& data) {
    Aws::S3::Model::PutObjectRequest putObjectRequest;
    putObjectRequest.SetBucket(bucket_name.c_str());
    putObjectRequest.SetKey(key.c_str());
    putObjectRequest.SetContentType("text/plain");

    auto dataStream = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    *dataStream << data;
    putObjectRequest.SetBody(dataStream);

    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    auto putOutcome = s3Client->PutObject(putObjectRequest);
    if (!putOutcome.IsSuccess()) {
        std::cout << "Error putting data into the bucket: " << bucket_name << " key: " << key << " data size: " << data.size() << std::endl;
    }
}

bool CloudClient::GetData(const std::string& bucket_name, const std::string& key, std::string& data) {
    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket(bucket_name.c_str());
    getObjectRequest.SetKey(key.c_str());

    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    try {
        auto getObjectOutcome = s3Client->GetObject(getObjectRequest);
        if (getObjectOutcome.IsSuccess()) {
            data = Aws::Utils::StringUtils::to_string(getObjectOutcome.GetResult().GetBody().rdbuf());
            return true;
        } else if (getObjectOutcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
            return false;
        } else {
            std::cout << "Error getting data " << key << " from the bucket: " << getObjectOutcome.GetError().GetMessage() << std::endl;
            std::ofstream outfile("/tmp/error.log", std::ios::out | std::ios::app);
            outfile << "Error getting data " << key << " from the bucket: " << getObjectOutcome.GetError().GetMessage() << std::endl;
            outfile.close(); 
            return false;
        }
    } catch (const std::exception& e) {
        std::cout << "Exception occured in s3client->getobject" << std::endl;
        std::cout << "Error getting data " << key << " from the bucket: " << e.what() << std::endl;
        return false;
    }
}

bool CloudClient::GetData(const std::string& bucket_name, const std::string& key, int offset, long long length, std::string& data) {
    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket(bucket_name.c_str());
    getObjectRequest.SetKey(key.c_str());
    getObjectRequest.SetRange("bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1));

    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    auto getObjectOutcome = s3Client->GetObject(getObjectRequest);

    if (getObjectOutcome.IsSuccess()) {
        data = Aws::Utils::StringUtils::to_string(getObjectOutcome.GetResult().GetBody().rdbuf());
        return true;
    } else if (getObjectOutcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
        return false;
    } else {
        std::cout << "Error getting data " << key << " from the bucket: " << getObjectOutcome.GetError().GetMessage() << std::endl;
        std::ofstream outfile("/tmp/error.log", std::ios::out | std::ios::app);
        outfile << "Error getting data " << key << " from the bucket: " << getObjectOutcome.GetError().GetMessage() << std::endl;
        outfile.close();
        return false;
    }
}

void CloudClient::DeleteData(const std::string& bucket_name, const std::string& key) {
    Aws::S3::Model::DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(bucket_name.c_str());
    deleteObjectRequest.SetKey(key.c_str());

    Aws::S3::S3Client* s3Client;
    {
        std::lock_guard<std::mutex> lock(s3ClientMtx);
        s3Client = s3Clients->at(s3ClientIndex);
        s3ClientIndex = (s3ClientIndex + 1) % nS3Clients;
    }

    auto deleteObjectOutcome = s3Client->DeleteObject(deleteObjectRequest);
    if (!deleteObjectOutcome.IsSuccess()) {
        std::ofstream outfile("/tmp/error.log", std::ios::out | std::ios::app);
        outfile << "Error deleting data from the bucket: " << bucket_name << " key: " << key << std::endl;
        outfile << "Error message: " << deleteObjectOutcome.GetError().GetMessage() << std::endl;
        outfile.close();
    }
}


void CloudClient::ConnectLambda(bool proxy) {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "us-east-1";

    if (proxy) {
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.proxyScheme = Aws::Http::Scheme::HTTP;
        clientConfig.proxyHost = "proxy.pdl.cmu.edu";
        clientConfig.proxyPort = 3128;
    }
    lambdaClient = new Aws::Lambda::LambdaClient(clientConfig);
}

void CloudClient::InvokeLambda(const std::string& functionName, const std::map<std::string, std::pair<std::string, std::string>>& mapPayload) {
    Aws::Lambda::Model::InvokeRequest invokeRequest;
    Aws::Utils::Json::JsonValue jsonPayload;
    for (const auto& [key, value] : mapPayload) {
        if (value.first == "long long") {
            jsonPayload.WithInt64(key.c_str(), std::stoll(value.second));
        } else if (value.first == "int") {
            jsonPayload.WithInteger(key.c_str(), std::stoi(value.second));
        } else if (value.first == "bool") {
            assert(value.second == "true" || value.second == "false");
            jsonPayload.WithBool(key.c_str(), value.second == "true");
        } else if (value.first == "string") {
            jsonPayload.WithString(key.c_str(), value.second.c_str());
        } else if (value.first == "double") {
            jsonPayload.WithDouble(key.c_str(), std::stod(value.second));
        } else {
            throw std::runtime_error("Unknown type: " + value.first);
        }
    }
    invokeRequest.SetFunctionName(functionName.c_str());
    invokeRequest.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    invokeRequest.SetLogType(Aws::Lambda::Model::LogType::Tail);
    std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("TriggerLabmdaFunction");
    *payload << jsonPayload.View().WriteReadable();
    invokeRequest.SetBody(payload);
    invokeRequest.SetContentType("application/json");

    auto invokeOutcome = lambdaClient->Invoke(invokeRequest);
    if (!invokeOutcome.IsSuccess()) {
        throw std::runtime_error("Error invoking lambda function: " + invokeOutcome.GetError().GetMessage());
    }
}


std::map<std::string, Aws::EC2::Model::InstanceType> CloudClient::instanceTypeMap = {
    { "r5.xlarge", Aws::EC2::Model::InstanceType::r5_xlarge }
};

void CloudClient::ConnectEC2(const std::string& region) {
    std::cout << "Connecting to EC2" << std::endl;
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::String(region);
    this->region = region;

    ec2Client = new Aws::EC2::EC2Client(clientConfig);
    if (!ec2Client) {
        throw std::runtime_error("Error creating EC2 client.");
    }
}

std::vector<std::string> CloudClient::createInstances(int n) {
    std::cout << "Creating EC2 instance" << std::endl;
    
    Aws::EC2::Model::RunInstancesRequest request;
    request.SetImageId(CACHE_ENGINE_IMG_ID.c_str());
    request.SetInstanceType(instanceTypeMap[CACHE_ENGINE_INSTANCE_TYPE]);
    request.SetKeyName(KEY_PAIR_NAME.c_str());
    request.SetSubnetId(SUBNET_ID.c_str());
    request.SetSecurityGroupIds({ SECURITY_GROUP_ID.c_str() });
    request.SetMinCount(n);
    request.SetMaxCount(n);

    auto outcome = ec2Client->RunInstances(request);
    if (!outcome.IsSuccess()) {
        std::cout << "Error creating EC2 instance: " << outcome.GetError().GetMessage() << std::endl;
        throw std::runtime_error("Error creating EC2 instance.");
    }

    std::vector<std::string> instanceIds;
    for (const auto& instance : outcome.GetResult().GetInstances()) {
        std::string id = instance.GetInstanceId();
        std::string ipAddress = instance.GetPrivateIpAddress();
        while (!isSshAccessible(ipAddress)) {
            std::cout << "Waiting for SSH to be accessible" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        instanceIds.push_back(id);
        idToIpAddress[id] = ipAddress;
    }

    return instanceIds;
}

bool CloudClient::isSshAccessible(const std::string& ipAddress) {
    std::string ssh_command = "ssh -o StrictHostKeyChecking=no ubuntu@" + ipAddress + " 'echo \"SSH successful\"'";
    return system(ssh_command.c_str()) == 0;
}

void CloudClient::deleteInstances(std::vector<std::string> instanceIds) {
    std::cout << "Deleting EC2 instance" << std::endl;
    Aws::EC2::Model::TerminateInstancesRequest request;
    Aws::Vector<Aws::String> ids;
    for (const auto& id : instanceIds) {
        ids.push_back(id.c_str());
    }
    request.SetInstanceIds(ids);

    auto outcome = ec2Client->TerminateInstances(request);
    if (!outcome.IsSuccess()) {
        throw std::runtime_error("Error deleting EC2 instance.");
    }

    for (const auto& id : instanceIds) {
        idToIpAddress.erase(id);
    }
}

std::vector<std::string> CloudClient::getIpAddresses(std::vector<std::string> instanceIds) {
    std::vector<std::string> ipAddresses;
    for (const auto& id : instanceIds) {
        ipAddresses.push_back(idToIpAddress[id]);
    }
    return ipAddresses;
}

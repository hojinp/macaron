#ifndef CLOUD_APIS_H
#define CLOUD_APIS_H

#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/ec2/EC2Client.h>
#include <aws/ec2/model/InstanceType.h>
#include <string>
#include <map>
#include <mutex>



class CloudClient {
public:
    CloudClient();
    void ConnectS3OnPrem(const std::string& endpointUrl);
    void ConnectS3Cloud(const std::string& region, bool proxy=false);
    void setTestBucket(std::string bucketName);
    void runTest(Aws::S3::S3Client* s3Client);
    void RenewS3Clients(bool proxy=false);
    bool DoesBucketExist(const std::string& bucketName);
    void CreateBucket(const std::string& bucketName);
    void ListBucket(const std::string& bucket_name, std::vector<std::string>& keys);
    void DeleteBucket(const std::string& bucketName);
    void PutData(const std::string& bucketName, const std::string& key, const std::string& data);
    bool GetData(const std::string& bucketName, const std::string& key, std::string& data);
    bool GetData(const std::string& bucket_name, const std::string& key, int offset, long long length, std::string& data);
    void DeleteData(const std::string& bucketName, const std::string& key);
    Aws::S3::Model::BucketLocationConstraint GetBucketLocationConstraint(const std::string& region);
    
    void ConnectLambda(bool proxy=false);
    void InvokeLambda(const std::string& functionName, const std::map<std::string, std::pair<std::string, std::string>>& mapPayload);

    void ConnectEC2(const std::string& region);
    std::vector<std::string> createInstances(int n);
    void deleteInstances(std::vector<std::string> instanceIds);
    std::vector<std::string> getIpAddresses(std::vector<std::string> instanceIds);

private:
    std::vector<Aws::S3::S3Client*> *s3Clients;
    int nS3Clients;
    int s3ClientIndex;
    std::mutex s3ClientMtx;
    std::thread s3ClientRenewThread;
    std::string testBucketName;

    Aws::Lambda::LambdaClient *lambdaClient;
    
    Aws::EC2::EC2Client *ec2Client;
    std::mutex mtx;
    std::map<std::string, std::string> idToIpAddress;
    bool isSshAccessible(const std::string& ipAddress);
    
    std::string region = "ONPREM";
    static bool apiInitialized;

    static std::map<std::string, Aws::EC2::Model::InstanceType> instanceTypeMap;
};

#endif // CLOUD_APIS_H


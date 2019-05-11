#include "License.h"

namespace zilliz {
namespace vecwise {
namespace server {

ServerError
LicenseSave(const std::string& path,const int& deviceCount,const std::vector<std::string>& shas)
{
    std::ofstream file(path);

    boost::archive::binary_oarchive oa(file);
    oa << deviceCount;
    for(int i=0;i<deviceCount;i++)
    {
        oa << shas[i];
    }
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LicenseLoad(const std::string& path,int& deviceCount,std::vector<std::string>& shas)
{
    std::ifstream file(path);

    boost::archive::binary_iarchive ia(file);
    ia >> deviceCount;
    std::string sha;
    for(int i=0;i<deviceCount;i++)
    {
        ia >> sha;
        shas.push_back(sha);
    }
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
Licensefileread(const std::string& path)
{
    std::ifstream fileread;
    fileread.open(path,std::ios::in);
    if(!fileread)
    {
        printf("Can't open file\n");
        return  SERVER_UNEXPECTED_ERROR;
    }
    fileread.close();
    return  SERVER_SUCCESS;
}

ServerError
Licensefileread (const std::string& path,std::ifstream& fileread){
    fileread.open(path,std::ios::in);
    if(!fileread)
    {
        printf("Can't open file\n");
        return  SERVER_UNEXPECTED_ERROR;
    }
    return  SERVER_SUCCESS;
}

ServerError
Licensefilewrite (const std::string& path,std::ofstream& filewrite) {
    filewrite.open(path,std::ios::out);
    if(!filewrite)
    {
        printf("Can't write file\n");
        return  SERVER_UNEXPECTED_ERROR;
    }
    return  SERVER_SUCCESS;
}

ServerError
LicenseGetCount(int &deviceCount)
{
    nvmlReturn_t result = nvmlInit();
    if (NVML_SUCCESS != result)
    {
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        return SERVER_UNEXPECTED_ERROR;
    }
    cudaError_t error_id = cudaGetDeviceCount(&deviceCount);
    if (error_id != cudaSuccess)
    {
        printf("cudaGetDeviceCount returned %d\n-> %s\n", (int)error_id, cudaGetErrorString(error_id));
        printf("Result = FAIL\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseGetuuid(int& deviceCount, std::vector<std::string>& uuids)
{
    nvmlReturn_t result = nvmlInit();
    if (NVML_SUCCESS != result)
    {
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        return SERVER_UNEXPECTED_ERROR;
    }
    cudaError_t error_id = cudaGetDeviceCount(&deviceCount);
    if (error_id != cudaSuccess)
    {
        printf("cudaGetDeviceCount returned %d\n-> %s\n", (int)error_id, cudaGetErrorString(error_id));
        printf("Result = FAIL\n");
        return SERVER_UNEXPECTED_ERROR;
    }

    if (deviceCount == 0)
    {
        printf("There are no available device(s) that support CUDA\n");
        return  SERVER_UNEXPECTED_ERROR;
    }

    for (int dev = 0; dev < deviceCount; ++dev)
    {
        nvmlDevice_t device;
        result = nvmlDeviceGetHandleByIndex(dev, &device);
        printf("device id: %d\n", dev);
        if (NVML_SUCCESS != result)
        {
            printf("Failed to get handle for device %i: %s\n", dev, nvmlErrorString(result));
            return SERVER_UNEXPECTED_ERROR;
        }

        char* uuid = (char*)malloc(80);
        unsigned int length = 80;
        nvmlReturn_t err = nvmlDeviceGetUUID(device, uuid, length);
        if(err != NVML_SUCCESS) {
            printf("nvmlDeviceGetUUID error: %d\n", err);
            return SERVER_UNEXPECTED_ERROR;
        }

        printf("\n device: %d, uuid = %s \n", dev, uuid);
        uuids.push_back(std::string(uuid));
        free(uuid);
        uuid = NULL;
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseGetuuidmd5(const int& deviceCount,std::vector<std::string>& uuids,std::vector<std::string>& md5s)
{
    MD5_CTX ctx;
    unsigned char outmd[16];
    char temp[2];
    std::string md5="";
    for(int dev=0;dev<deviceCount;dev++)
    {
        md5.clear();
        memset(outmd,0, sizeof(outmd));
        MD5_Init(&ctx);
        MD5_Update(&ctx,uuids[dev].c_str(),uuids[dev].size());
        MD5_Final(outmd,&ctx);
        for(int i=0;i<16;i++)
        {
            std::sprintf(temp,"%02X",outmd[i]);
            md5+=temp;
        }
        md5s.push_back(md5);
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseGetfilemd5(const std::string& path,std::string& filemd5)
{
//    MD5_CTX ctx;
//    unsigned char outmd[16];
//    char temp[2];
//    char* buffer = (char*)malloc(1024);
//    std::ifstream fileread;
//    Licensefileread(path,fileread);
//    memset(outmd,0,sizeof(outmd));
//    memset(buffer,0,sizeof(buffer));
//    MD5_Init(&ctx);
//    while(!fileread.eof()) {
//        fileread.get(buffer,1024,EOF);
//        MD5_Update(&ctx, buffer, strlen(buffer));
//        memset(buffer, 0, sizeof(buffer));
//    }
//    MD5_Final(outmd,&ctx);
//    for(int i=0;i<16;i++)
//    {
//        std::sprintf(temp,"%02X",outmd[i]);
//        filemd5+=temp;
//    }
//    free(buffer);
//    buffer=NULL;
//    fileread.close();
//    return SERVER_SUCCESS;


    filemd5.clear();

    std::ifstream file(path.c_str(), std::ifstream::binary);
    if (!file)
    {
        return -1;
    }

    MD5_CTX md5Context;
    MD5_Init(&md5Context);

    char buf[1024 * 16];
    while (file.good()) {
        file.read(buf, sizeof(buf));
        MD5_Update(&md5Context, buf, file.gcount());
    }

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5_Final(result, &md5Context);

    char hex[35];
    memset(hex, 0, sizeof(hex));
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i)
    {
        sprintf(hex + i * 2, "%02X", result[i]);
    }
    hex[32] = '\0';
    filemd5 = std::string(hex);

    return SERVER_SUCCESS;
}

ServerError
LicenseGetuuidsha(const int& deviceCount,std::vector<std::string>& uuids,std::vector<std::string>& shas)
{
    SHA256_CTX ctx;
    unsigned char outmd[32];
    char temp[2];
    std::string sha="";
    for(int dev=0;dev<deviceCount;dev++)
    {
        sha.clear();
        memset(outmd,0, sizeof(outmd));
        SHA256_Init(&ctx);
        SHA256_Update(&ctx,uuids[dev].c_str(),uuids[dev].size());
        SHA256_Final(outmd,&ctx);
        for(int i=0;i<32;i++)
        {
            std::sprintf(temp,"%02X",outmd[i]);
            sha += temp;
        }
        shas.push_back(sha);
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseGetfiletime(const std::string& path,time_t& last_time)
{
    struct stat buf;
    stat(path.c_str(),&buf);
    last_time =buf.st_mtime;
    return SERVER_SUCCESS;
}

ServerError
LicenseGetfilesize(const std::string& path,off_t& file_size)
{
    struct stat buf;
    if(stat(path.c_str(),&buf)==-1)
    {
        printf(" GET fiel_size is wrong");
        return  SERVER_UNEXPECTED_ERROR;
    }
    file_size =buf.st_size;
    return SERVER_SUCCESS;
}

ServerError
LicenseLegalitycheck(const std::string& path)
{
    int deviceCount;
    std::vector<std::string> uuids;
    LicenseGetuuid(deviceCount,uuids);
    std::vector<std::string> shas;
    LicenseGetuuidsha(deviceCount,uuids,shas);


    int deviceCountcheck;
    std::vector<std::string> shascheck;
    LicenseLoad(path,deviceCountcheck,shascheck);

    if(deviceCount!=deviceCountcheck)
    {
        printf("deviceCount is wrong\n");
        return  SERVER_UNEXPECTED_ERROR;
    }
    for(int i=0;i<deviceCount;i++)
    {
        if(shascheck[i]!=shas[i])
        {
            printf("uuidsha %d is wrong\n",i);
            return  SERVER_UNEXPECTED_ERROR;
        }
    }

    return SERVER_SUCCESS;
}

ServerError
LicensefileSave(const std::string& path,const std::string& path2){

    std::string filemd5;
    LicenseGetfilemd5(path,filemd5);
    std::ofstream file(path2);
    time_t last_time;
    LicenseGetfiletime(path,last_time);

    boost::archive::binary_oarchive oa(file);
    oa << filemd5;
    oa << last_time;
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LicensefileLoad(const std::string& path2,std::string& filemd5,time_t& last_time){

    std::ifstream file(path2);

    boost::archive::binary_iarchive ia(file);
    ia >> filemd5;
    ia >> last_time;
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LicenseIntegritycheck(const std::string& path,const std::string& path2)
    {
        std::string filemd5;
        LicenseGetfilemd5(path,filemd5);
        time_t  last_time;
        LicenseGetfiletime(path,last_time);

        time_t last_timecheck;
        std::string filemd5check;
        LicensefileLoad(path2,filemd5check,last_timecheck);

        if(filemd5!=filemd5check)
        {
            printf("This file has been modified\n");
            return SERVER_UNEXPECTED_ERROR;
        }
        if(last_time!=last_timecheck)
        {
            printf("last_time is wrong\n");
            return  SERVER_UNEXPECTED_ERROR;
        }

        return SERVER_SUCCESS;
    }

ServerError
LicenseValidate(const std::string& path) {


    return SERVER_SUCCESS;
}



}
}
}
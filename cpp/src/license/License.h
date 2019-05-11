#pragma once

#include "utils/Error.h"
#include <cuda.h>
#include <memory>
#include <iostream>
#include <vector>
#include <string.h>
#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/binary_iarchive.hpp"

#include <cuda_runtime.h>
#include <nvml.h>
#include <openssl/md5.h>
#include <openssl/sha.h>


namespace zilliz {
namespace vecwise {
namespace server {


ServerError
LicenseSave(const std::string& path,const int& deviceCount,const std::vector<std::string>& shas);

ServerError
LicenseLoad(const std::string& path,int& deviceCount,std::vector<std::string>& shas);

ServerError
Licensefileread (const std::string& path,std::ifstream& fileread);

ServerError
Licensefilewrite (const std::string& path,std::ofstream& filewrite);

ServerError
LicenseGetuuid(int& deviceCount, std::vector<std::string>& uuids);

ServerError
LicenseGetuuidmd5(const int& deviceCount,std::vector<std::string>& uuids,std::vector<std::string>& md5s);

ServerError
LicenseGetfilemd5(const std::string& path,std::string& filemd5);

ServerError
LicenseGetuuidsha(const int& deviceCount,std::vector<std::string>& uuids,std::vector<std::string>& shas);

ServerError
LicenseIntegritycheck(const std::string& path,const std::string& path2);

ServerError
LicenseGetfiletime(const std::string& path,time_t& last_time);

ServerError
LicenseLegalitycheck(const std::string& path);

ServerError
LicensefileSave(const std::string& path,const std::string& path2);

ServerError
LicensefileLoad(const std::string& path2,std::string& filemd5,time_t& last_time);


ServerError
LicenseGetfilesize(const std::string& path,off_t& file_size);

ServerError
LicenseValidate(const std::string& path);

ServerError
Licensefileread(const std::string& path);

ServerError
LicenseGetCount(int &deviceCount);
}
}
}



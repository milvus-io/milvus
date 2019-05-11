//
// Created by zilliz on 19-5-10.
//

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
#include <map>
#include <list>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/foreach.hpp>
#include  <boost/serialization/vector.hpp>
#include  <boost/serialization/map.hpp>

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <cuda_runtime.h>
#include <nvml.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

namespace zilliz {
namespace vecwise {
namespace server {

class BoostArchive;
class Licensedata1;


class Licensefiledata;
class STLlicensefiledata;


ServerError
LiSave(const std::string& path,const int& deviceCount,const std::map<int,std::string>& uuidEncryption);

ServerError
LiLoad(const std::string& path,int& deviceCount,std::map<int,std::string>& uuidEncryption,int64_t& RemainingTime);

ServerError
LifileSave(const std::string& path,const time_t& update_time,const off_t& file_size,const std::string& filemd5);

ServerError
LifileLoad(const std::string& path,time_t& update_time,off_t& file_size,std::string& filemd5);

ServerError
LiSave(const std::string& path,const int& deviceCount,const std::map<int,std::string>& uuidEncryption, const int64_t& RemainingTime);

void Alterfile(const std::string &path1, const std::string &path2 ,const boost::system::error_code &ec, boost::asio::deadline_timer* pt);
void Runtime(const std::string &path1, const std::string &path2 );

//    void Print(const boost::system::error_code &ec,boost::asio::deadline_timer* pt, int * pcount );
//    void Run();

}
}
}
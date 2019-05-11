//
// Created by zilliz on 19-5-10.
//

#include "LicensePublic.h"
#include "license/License.h"
//#include "BoostArchive.h"
#define  RTime 100

using std::string;
using std::map;
namespace zilliz {
namespace vecwise {
namespace server {

// GET /tmp/vecwise_engine.license
class Licensedata1
{
public:
    Licensedata1()
    :_deviceCount(0)
    ,_RemainingTime(RTime)
    {}

    Licensedata1(const int& deviceCount,const map<int,string>& uuidEncryption)
    :_deviceCount(deviceCount)
    ,_uuidEncryption(uuidEncryption)
    ,_RemainingTime(RTime)
    {}

    Licensedata1(const int& deviceCount,const map<int,string>& uuidEncryption,const int64_t& RemainingTime)
    :_deviceCount(deviceCount)
    ,_uuidEncryption(uuidEncryption)
    ,_RemainingTime(RemainingTime)
    {}

    int GetdeviceCount()
    {
        return  _deviceCount;
    }
    map<int,string> GetuuidEncryption()
    {
        return  _uuidEncryption;
    }
    int64_t GetRemainingTime()
    {
        return  _RemainingTime;
    }

private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & _deviceCount;
        ar & _uuidEncryption;
        ar & _RemainingTime;
    }

public:
    int _deviceCount;
    map<int,string> _uuidEncryption;
    int64_t _RemainingTime;
};


class STLlicensedata
{
private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & m_licensedata;
    }

public:
    Licensedata1*  m_licensedata;
};


ServerError
LiSave(const string& path,const int& deviceCount,const map<int,string>& uuidEncryption)
{
    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<Licensedata1>();
    STLlicensedata stllicensedata;

    Licensedata1 *p = new Licensedata1(deviceCount,uuidEncryption);
    stllicensedata.m_licensedata = p;
    oa << stllicensedata;

    delete p;
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LiSave(const string& path,const int& deviceCount,const map<int,string>& uuidEncryption, const int64_t& RemainingTime)
{
    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<Licensedata1>();
    STLlicensedata stllicensedata;

    Licensedata1 *p = new Licensedata1(deviceCount,uuidEncryption,RemainingTime);
    stllicensedata.m_licensedata = p;
    oa << stllicensedata;

    delete p;
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LiLoad(const string& path,int& deviceCount,map<int,string>& uuidEncryption,int64_t& RemainingTime)
{
    std::ifstream file(path);
    boost::archive::binary_iarchive ia(file);
    ia.register_type<Licensedata1>();
    STLlicensedata stllicensedata;
    ia >> stllicensedata;
    deviceCount = stllicensedata.m_licensedata->GetdeviceCount();
    uuidEncryption = stllicensedata.m_licensedata->GetuuidEncryption();
    RemainingTime = stllicensedata.m_licensedata->GetRemainingTime();
    file.close();
    return  SERVER_SUCCESS;
}


// GET /tmp/vecwise_engine2.license

class Licensefiledata
{
public:
    Licensefiledata()
            :_update_time(0)
            ,_file_size(0)
            ,_filemd5("")
    {}

    Licensefiledata(const time_t& update_time,const off_t& file_size,const string& filemd5)
            :_update_time(update_time)
            ,_file_size(file_size)
            ,_filemd5(filemd5)
    {}

    time_t Getupdate_time()
    {
        return  _update_time;
    }
    off_t Getfile_size()
    {
        return  _file_size;
    }
    string Getfilemd5()
    {
        return  _filemd5;
    }

private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & _update_time;
        ar & _file_size;
        ar & _filemd5;
    }

public:
    time_t _update_time;
    off_t  _file_size;
    string _filemd5;
};


class STLlicensefiledata
{
private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & m_licensefiledata;
    }

public:
    Licensefiledata*  m_licensefiledata;
};


ServerError
LifileSave(const string& path,const time_t& update_time,const off_t& file_size,const string& filemd5)
{
    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<Licensefiledata>();
    STLlicensefiledata stllicensefiledata;

    Licensefiledata *p = new Licensefiledata(update_time,file_size,filemd5);
    stllicensefiledata.m_licensefiledata = p;
    oa << stllicensefiledata;

    delete p;
    file.close();
    return  SERVER_SUCCESS;
}

ServerError
LifileLoad(const string& path,time_t& update_time,off_t& file_size,string& filemd5)
    {
        std::ifstream file(path);
        boost::archive::binary_iarchive ia(file);
        ia.register_type<Licensefiledata>();
        STLlicensefiledata stllicensefiledata;
        ia >> stllicensefiledata;
        update_time = stllicensefiledata.m_licensefiledata->Getupdate_time();
        file_size = stllicensefiledata.m_licensefiledata->Getfile_size();
        filemd5 = stllicensefiledata.m_licensefiledata->Getfilemd5();
        file.close();
        return  SERVER_SUCCESS;
    }


void Alterfile(const string &path1, const string &path2 ,const boost::system::error_code &ec, boost::asio::deadline_timer* pt)
{
    int deviceCount;
    map<int,string> uuidEncryption;
    int64_t RemainingTime;
    LiLoad(path1,deviceCount,uuidEncryption,RemainingTime);

    std::cout<< "RemainingTime: " << RemainingTime <<std::endl;

    if(RemainingTime==0)
    {
        exit(1);
    }
    RemainingTime--;
    LiSave(path1,deviceCount,uuidEncryption,RemainingTime);

    int deviceCountcheck;
    map<int,string> uuidEncryptioncheck;
    int64_t RemainingTimecheck;
    LiLoad(path1,deviceCountcheck,uuidEncryptioncheck,RemainingTimecheck);

    std::cout<< "RemainingTimecheck: " << RemainingTimecheck <<std::endl;

    time_t update_time;
    LicenseGetfiletime(path1,update_time);
    off_t file_size;
    LicenseGetfilesize(path1,file_size);
    string filemd5;
    LicenseGetfilemd5(path1,filemd5);

    std::cout<< "filemd5: " << filemd5 <<std::endl;

    LifileSave(path2,update_time,file_size,filemd5);
    time_t update_timecheck;
    off_t file_sizecheck;
    string filemd5check;
    LifileLoad(path2,update_timecheck,file_sizecheck,filemd5check);

    std::cout<< "update_timecheck: " << update_timecheck <<std::endl;
    std::cout<< "file_sizecheck: " << file_sizecheck <<std::endl;
    std::cout<< "filemd5check: " << filemd5check <<std::endl;

    pt->expires_at(pt->expires_at() + boost::posix_time::hours(1)) ;
    pt->async_wait(boost::bind(Alterfile,path1,path2, boost::asio::placeholders::error, pt));

}
void Runtime(const string &path1, const string &path2 )
{
    boost::asio::io_service io;
    boost::asio::deadline_timer t(io, boost::posix_time::hours(1));
    t.async_wait(boost::bind(Alterfile,path1,path2, boost::asio::placeholders::error, &t));
    io.run();
    return;;
}

}
}
}
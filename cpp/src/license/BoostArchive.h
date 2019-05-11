//
// Created by zilliz on 19-5-10.
//

#ifndef VECWISE_ENGINE_BOOSTARCHIVE_H
#define VECWISE_ENGINE_BOOSTARCHIVE_H
#include <list>
#include <fstream>
#include <string>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using std::list;
using std::ifstream;
using std::ofstream;
using std::string;
using std::map;

template <class T>
class BoostArchive
{
public:
    typedef T entity_type;
    typedef boost::archive::binary_iarchive InputArchive;
    typedef boost::archive::binary_oarchive OutputArchive;

    BoostArchive(const string & archive_file_path)
            : _file_path_name(archive_file_path)
            , _p_ofs(NULL)
            , _p_output_archive(NULL)
            , _entity_nums(0)
    {
        load_arvhive_info();
    }
    ~BoostArchive()
    {
        close_output();
    }
    //存储一个对象，序列化
    void store(const entity_type & entity);

    //反序列化， 提取所有对象
    bool restore(list<entity_type> & entitys);

    size_t size() const
    {
        return _entity_nums;
    }

private:
    void save_archive_info()    //保存已序列化的对象个数信息
    {
        ofstream ofs;
        ofs.open(get_archive_info_file_path(),std::ios::out | std::ios::trunc);
        if (ofs.is_open())
        {
            ofs << _entity_nums;
        }
        ofs.close();
    }

    void load_arvhive_info()//读取已序列化的对象个数信息
    {
        ifstream ifs;
        ifs.open(get_archive_info_file_path(),std::ios_base::in);
        if (ifs.is_open() && !ifs.eof())
        {
            int enity_num = 0;
            ifs >> enity_num;
            _entity_nums = enity_num;
        }
        ifs.close();
    }

    string get_archive_info_file_path()
    {
        return "/tmp/vecwise_engine.meta";
    }

    void close_output()
    {
        if (NULL != _p_output_archive)
        {
            delete _p_output_archive;
            _p_output_archive = NULL;
            save_archive_info();
        }
        if (NULL != _p_ofs)
        {
            delete _p_ofs;
            _p_ofs = NULL;
        }
    }

private:
    size_t _entity_nums;
    string _file_path_name;
    ofstream * _p_ofs;
    OutputArchive * _p_output_archive;
};

template <class T>
bool BoostArchive<T>::restore( list<entity_type> & entitys )
{
    close_output();
    load_arvhive_info();
    ifstream ifs(_file_path_name);
    if (ifs)
    {
        InputArchive ia(ifs);
        for (size_t cnt = 0; cnt < _entity_nums; ++cnt)
        {
            entity_type entity;
            ia & entity;
            entitys.push_back(entity);
        }
        return true;
    }
    return false;
}

template <class T>
void BoostArchive<T>::store( const entity_type & entity )
{
    if (NULL == _p_output_archive)
    {
        _p_ofs = new ofstream(_file_path_name);
        _p_output_archive = new OutputArchive(*_p_ofs);
    }
    (*_p_output_archive) & entity;
    ++_entity_nums;
}
#endif //VECWISE_ENGINE_BOOSTARCHIVE_H

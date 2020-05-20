#pragma once
#include "ReferenceProxy.h"
#include <string>

template <typename ...Fields>
class DBBaseResource : public ReferenceProxy,
                       public Fields... {
public:
    DBBaseResource(const Fields&... fields);

    virtual std::string ToString() const;

    virtual ~DBBaseResource() {}
};

template <typename ...Fields>
DBBaseResource<Fields...>::DBBaseResource(const Fields&... fields) : Fields(fields)... {
    /* InstallField("id"); */
    /* InstallField("status"); */
    /* InstallField("created_on"); */
    /* std::vector<std::string> attrs = {Fields::ATTR...}; */
}

template <typename ...Fields>
std::string DBBaseResource<Fields...>::ToString() const {
}

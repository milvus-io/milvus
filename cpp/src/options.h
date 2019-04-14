#ifndef VECENGINE_OPTIONS_H_
#define VECENGINE_OPTIONS_H_

#include <string>

namespace vecengine {

struct Options {

}; // Options


struct GroupOptions {
    size_t dimension;
    bool has_id = false;
}; // GroupOptions


struct MetaOptions {
}; // MetaOptions


struct DBMetaOptions : public MetaOptions {
    std::string db_uri;
}; // DBMetaOptions


} // namespace vecengine

#endif // VECENGINE_OPTIONS_H_

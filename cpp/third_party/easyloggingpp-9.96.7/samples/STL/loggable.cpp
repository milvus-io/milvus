 //
 // This file is part of Easylogging++ samples
 //
 // Usage sample of el::Loggable to make class log-friendly
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class MyClass : public el::Loggable {
public:
    MyClass(const std::string& name) : m_name(name) {}

    virtual inline void log(el::base::type::ostream_t& os) const {
        os << m_name.c_str();
    }


private:
    std::string m_name;
};

int main(void) {
    MyClass c("c"); 
    MyClass c2("c2"); 
    LOG(INFO) << "I am " << c << "; and I am " << c2;
    return 0;
}

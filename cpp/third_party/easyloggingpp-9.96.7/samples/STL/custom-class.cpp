 //
 // This file is part of EasyLogging++ samples
 //
 // Demonstration of logging your own class, a bit similar to containers.cpp but specific to custom class only
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include <sstream>
#include <vector>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class Vehicle : public el::Loggable {
    public:
        Vehicle(const std::string& make_, const std::string& model_, unsigned int year_ = 2013,
                    const std::string& version_ = "") :
            make_(make_), model_(model_), year_(year_), version_(version_) {}

        virtual void log(el::base::type::ostream_t& os) const;

    private:
        std::string make_;
        std::string model_;
        int year_;
        std::string version_;
};

void Vehicle::log(el::base::type::ostream_t& os) const {
    os << "(" << make_.c_str() << " " << model_.c_str() << " " << year_ << (version_.size() > 0 ? " " : "") << version_.c_str() << ")";
}

int main(void) {

    Vehicle vehicle1("Land Rover", "Discovery 4", 2013, "TD HSE");
    Vehicle vehicle2("Honda", "Accord", 2013, "V6 Luxury");
    Vehicle vehicle3("Honda", "Accord", 2010);

    LOG(INFO) << "We have vehicles available: " << vehicle1  << " " << vehicle2 << " " << vehicle3;

    std::vector<Vehicle*> vehicles;
    vehicles.push_back(&vehicle1);
    vehicles.push_back(&vehicle2);
    vehicles.push_back(&vehicle3);
    LOG(INFO) << "Now printing whole vector of pointers";
    LOG(INFO) << vehicles;

    std::vector<Vehicle> vehiclesStack;
    vehiclesStack.push_back(vehicle1);
    vehiclesStack.push_back(vehicle2);
    vehiclesStack.push_back(vehicle3);
    LOG(INFO) << "Now printing whole vector of classes";
    LOG(INFO) << vehiclesStack;


    return 0;
}


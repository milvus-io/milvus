 //
 // This file is part of Easylogging++ samples
 //
 // Logs different STL containers, some containing STL templates and other containing our own class Vehicle
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"
#include <sstream>
INITIALIZE_EASYLOGGINGPP

class Vehicle : public el::Loggable {
    public:
        Vehicle(const std::string& make_, const std::string& model_, unsigned int year_ = 2013,
                    const std::string& version_ = "") :
            make_(make_), model_(model_), year_(year_), version_(version_) {}
        virtual ~Vehicle() {}

        std::string toString(void) const {
            std::stringstream ss;
            ss << "[" << make_ << " " << model_ << " " << year_ << (version_.size() > 0 ? " " : "") << version_ << "]";
            return ss.str();
        }
        virtual void log(el::base::type::ostream_t& os) const {
            os << toString().c_str(); 
        }
    private:
        std::string make_;
        std::string model_;
        int year_;
        std::string version_;
};
void vectorLogs() {
  std::vector<std::string> stringVec;
  std::vector<Vehicle> vehicleVec;
  stringVec.push_back("stringVec");
  vehicleVec.push_back(Vehicle("Honda", "Accord", 2013, "vehicleVec")); 
  LOG(INFO) << "stringVec : " << stringVec; 
  LOG(INFO) << "vehicleVec : " << vehicleVec; 
}

void listLogs() {
  std::list<std::string> stringList;
  std::vector<std::string*> stringPtrList;
  std::list<Vehicle> vehicleList;
  std::vector<Vehicle*> vehiclePtrList;
  stringList.push_back("stringList");
  stringPtrList.push_back(new std::string("stringPtrList"));
  vehicleList.push_back(Vehicle("Honda", "Accord", 2013, "vehicleList"));
  vehiclePtrList.push_back(new Vehicle("Honda", "Accord", 2013, "vehiclePtrList"));
  LOG(INFO) << "stringList : " << stringList;
  LOG(INFO) << "stringPtrList : " << stringPtrList;
  LOG(INFO) << "vehicleList : " << vehicleList;
  LOG(INFO) << "vehiclePtrList : " << vehiclePtrList;
  
  delete stringPtrList.at(0);
  delete vehiclePtrList.at(0);
}

void otherContainerLogs() {
    std::map<int, std::string> map_;
    map_.insert (std::pair<int, std::string>(1, "one"));
    map_.insert (std::pair<int, std::string>(2, "two"));
    LOG(INFO) << "Map: " << map_;

    std::queue<int> queue_;
    queue_.push(77);
    queue_.push(16);
    LOG(INFO) << queue_;

    std::bitset<10> bitset_ (std::string("10110"));
    LOG(INFO) << bitset_;

    int pqueueArr_[]= { 10, 60, 50, 20 };
    std::priority_queue< int, std::vector<int>, std::greater<int> > pqueue (pqueueArr_, pqueueArr_ + 4);
    LOG(INFO) << pqueue;

    std::deque<int> mydeque_ (3,100);
    mydeque_.at(1) = 200;

    std::stack<int> stack_ (mydeque_);
    LOG(INFO) << stack_;

    std::stack<std::string*> stackStr_;
    stackStr_.push (new std::string("test"));
    LOG(INFO) << stackStr_;
    delete stackStr_.top();
}

int main(void) {
    vectorLogs();
    listLogs();
    otherContainerLogs();
    return 0;
}

 //
 //
 // This file is part of Easylogging++ samples
 // Send to network log
 //
 // Revision 1.0
 // @author mkhan3189
 //
 // Compile: sh compile.sh network-logger.cpp
 //

#include "easylogging++.h"

#include <boost/asio.hpp>

INITIALIZE_EASYLOGGINGPP


class Client
{
    boost::asio::io_service* io_service;
    boost::asio::ip::tcp::socket socket;

public:
    Client(boost::asio::io_service* svc, const std::string& host, const std::string& port) 
        : io_service(svc), socket(*io_service) 
    {
        boost::asio::ip::tcp::resolver resolver(*io_service);
        boost::asio::ip::tcp::resolver::iterator endpoint = resolver.resolve(boost::asio::ip::tcp::resolver::query(host, port));
        boost::asio::connect(this->socket, endpoint);
    };

    void send(std::string const& message) {
        socket.send(boost::asio::buffer(message));
    }
};

class NetworkDispatcher : public el::LogDispatchCallback
{
public:
    void updateServer(const std::string& host, int port) {
        m_client = std::unique_ptr<Client>(new Client(&m_svc, host, std::to_string(port)));
    }
protected:
  void handle(const el::LogDispatchData* data) noexcept override {
      m_data = data;
      // Dispatch using default log builder of logger
      dispatch(m_data->logMessage()->logger()->logBuilder()->build(m_data->logMessage(),
                 m_data->dispatchAction() == el::base::DispatchAction::NormalLog));
  }
private:
  const el::LogDispatchData* m_data;
  boost::asio::io_service m_svc;
  std::unique_ptr<Client> m_client;
  
  void dispatch(el::base::type::string_t&& logLine) noexcept
  {
      m_client->send(logLine);
  }
};


int main() {
    el::Helpers::installLogDispatchCallback<NetworkDispatcher>("NetworkDispatcher");
    // you can uninstall default one by
    // el::Helpers::uninstallLogDispatchCallback<el::base::DefaultLogDispatchCallback>("DefaultLogDispatchCallback");
    // Set server params
    NetworkDispatcher* dispatcher = el::Helpers::logDispatchCallback<NetworkDispatcher>("NetworkDispatcher");
    dispatcher->setEnabled(true);
    dispatcher->updateServer("127.0.0.1", 9090);
      
    // Start logging and normal program...
    LOG(INFO) << "First network log";
        
    // You can even use a different logger, say "network" and send using a different log pattern
}

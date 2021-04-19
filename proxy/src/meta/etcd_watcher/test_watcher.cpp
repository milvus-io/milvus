// Steps to test this file:
// 1. start a etcdv3 server
// 2. run this test
// 3. modify test key using etcdctlv3 or etcd-clientv3(Must using v3 api)
// TODO: move this test to unittest

#include "Watcher.h"

using namespace milvus::master;
int main() {
  try {
    Watcher watcher("127.0.0.1:2379", "SomeKey", [](etcdserverpb::WatchResponse res) {
      std::cerr << "Key1 changed!" << std::endl;
      std::cout << "Event size: " << res.events_size() << std::endl;
      for (auto &event: res.events()) {
        std::cout <<
                  event.kv().key() << ":" <<
                  event.kv().value() << std::endl;
      }
    }, false);
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(60000));
    watcher.Cancel();
    break;
  }
  }
  catch (const std::exception &e) {
    std::cout << e.what();
  }

}

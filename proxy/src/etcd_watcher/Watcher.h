#pragma once
#include "grpc/etcd.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <thread>

namespace milvus {
namespace master {

class AsyncWatchAction;

class Watcher {
 public:
  Watcher(std::string const &address,
          std::string const &key,
          std::function<void(etcdserverpb::WatchResponse)> callback);
  void Cancel();
  ~Watcher();

 private:
  std::unique_ptr<etcdserverpb::Watch::Stub> stub_;
  std::unique_ptr<AsyncWatchAction> call_;
  std::thread work_thread_;
};

class AsyncWatchAction {
 public:
  AsyncWatchAction(const std::string &key, etcdserverpb::Watch::Stub* stub);
  void WaitForResponse(std::function<void(etcdserverpb::WatchResponse)> callback);
  void CancelWatch();
 private:
  // Status status;
  grpc::ClientContext context_;
  grpc::CompletionQueue cq_;
  etcdserverpb::WatchResponse reply_;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> stream_;
  std::atomic<bool> cancled_ = false;
};
}
}
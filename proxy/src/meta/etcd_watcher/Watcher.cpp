#include "Watcher.h"

#include <memory>
#include <utility>
#include "grpc/etcd.grpc.pb.h"

namespace milvus {
namespace master {

Watcher::Watcher(const std::string &address,
                 const std::string &key,
                 std::function<void(etcdserverpb::WatchResponse)> callback,
                 bool with_prefix) {
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  stub_ = etcdserverpb::Watch::NewStub(channel);

  call_ = std::make_unique<AsyncWatchAction>(key, with_prefix, stub_.get());
  work_thread_ = std::thread([&]() {
    call_->WaitForResponse(callback);
  });
}

void Watcher::Cancel() {
  call_->CancelWatch();
}

Watcher::~Watcher() {
  Cancel();
  work_thread_.join();
}

AsyncWatchAction::AsyncWatchAction(const std::string &key, bool with_prefix, etcdserverpb::Watch::Stub *stub) {
  // tag `1` means to wire a rpc
  stream_ = stub->AsyncWatch(&context_, &cq_, (void *) 1);
  etcdserverpb::WatchRequest req;
  req.mutable_create_request()->set_key(key);
  if (with_prefix) {
    std::string range_end(key);
    int ascii = (int) range_end[range_end.length() - 1];
    range_end.back() = ascii + 1;
    req.mutable_create_request()->set_range_end(range_end);
  }
  void *got_tag;
  bool ok = false;
  if (cq_.Next(&got_tag, &ok) && ok && got_tag == (void *) 1) {
    // tag `2` means write watch request to stream
    stream_->Write(req, (void *) 2);
  } else {
    throw std::runtime_error("failed to create a watch connection");
  }

  if (cq_.Next(&got_tag, &ok) && ok && got_tag == (void *) 2) {
    stream_->Read(&reply_, (void *) this);
  } else {
    throw std::runtime_error("failed to write WatchCreateRequest to server");
  }
}

void AsyncWatchAction::WaitForResponse(std::function<void(etcdserverpb::WatchResponse)> callback) {
  void *got_tag;
  bool ok = false;

  while (cq_.Next(&got_tag, &ok)) {
    if (!ok) {
      break;
    }
    if (got_tag == (void *) 3) {
      cancled_.store(true);
      cq_.Shutdown();
      break;
    } else if (got_tag == (void *) this) // read tag
    {
      if (reply_.events_size()) {
        callback(reply_);
      }
      stream_->Read(&reply_, (void *) this);
    }
  }
}

void AsyncWatchAction::CancelWatch() {
  if (!cancled_.load()) {
    // tag `3` mean write done
    stream_->WritesDone((void *) 3);
    cancled_.store(true);
  }
}

}
}
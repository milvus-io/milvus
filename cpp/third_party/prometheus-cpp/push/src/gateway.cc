
#include "prometheus/gateway.h"

#include <memory>
#include <sstream>

#include "prometheus/client_metric.h"
#include "prometheus/serializer.h"
#include "prometheus/text_serializer.h"

#include <curl/curl.h>

namespace prometheus {

static const char CONTENT_TYPE[] =
    "Content-Type: text/plain; version=0.0.4; charset=utf-8";

Gateway::Gateway(const std::string host, const std::string port,
                 const std::string jobname, const Labels& labels,
                 const std::string username, const std::string password) {
  /* In windows, this will init the winsock stuff */
  curl_global_init(CURL_GLOBAL_ALL);

  std::stringstream jobUriStream;
  jobUriStream << host << ':' << port << "/metrics/job/" << jobname;
  jobUri_ = jobUriStream.str();

  if (!username.empty()) {
    auth_ = username + ":" + password;
  }

  std::stringstream labelStream;
  for (auto& label : labels) {
    labelStream << "/" << label.first << "/" << label.second;
  }
  labels_ = labelStream.str();
}

Gateway::~Gateway() { curl_global_cleanup(); }

const Gateway::Labels Gateway::GetInstanceLabel(std::string hostname) {
  if (hostname.empty()) {
    return Gateway::Labels{};
  }
  return Gateway::Labels{{"instance", hostname}};
}

void Gateway::RegisterCollectable(const std::weak_ptr<Collectable>& collectable,
                                  const Labels* labels) {
  std::stringstream ss;

  if (labels) {
    for (auto& label : *labels) {
      ss << "/" << label.first << "/" << label.second;
    }
  }

  collectables_.push_back(std::make_pair(collectable, ss.str()));
}

int Gateway::performHttpRequest(HttpMethod method, const std::string& uri,
                                const std::string& body) const {
  auto curl = curl_easy_init();
  if (!curl) {
    return -CURLE_FAILED_INIT;
  }

  curl_easy_setopt(curl, CURLOPT_URL, uri.c_str());

  curl_slist* header_chunk = nullptr;

  if (!body.empty()) {
    header_chunk = curl_slist_append(nullptr, CONTENT_TYPE);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_chunk);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body.size());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
  }

  if (!auth_.empty()) {
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, auth_.c_str());
  }

  switch (method) {
    case HttpMethod::Post:
      curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
      curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
      break;

    case HttpMethod::Put:
      curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
      break;

    case HttpMethod::Delete:
      curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
      curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
      break;
  }

  auto curl_error = curl_easy_perform(curl);

  long response_code;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

  curl_easy_cleanup(curl);
  curl_slist_free_all(header_chunk);

  if (curl_error != CURLE_OK) {
    return -curl_error;
  }

  return response_code;
}

std::string Gateway::getUri(const CollectableEntry& collectable) const {
  std::stringstream uri;
  uri << jobUri_ << labels_ << collectable.second;

  return uri.str();
}

int Gateway::Push() { return push(HttpMethod::Post); }

int Gateway::PushAdd() { return push(HttpMethod::Put); }

int Gateway::push(HttpMethod method) {
  const auto serializer = TextSerializer{};

  for (auto& wcollectable : collectables_) {
    auto collectable = wcollectable.first.lock();
    if (!collectable) {
      continue;
    }

    auto metrics = collectable->Collect();
    auto body = serializer.Serialize(metrics);
    auto uri = getUri(wcollectable);
    auto status_code = performHttpRequest(method, uri, body);

    if (status_code < 100 || status_code >= 400) {
      return status_code;
    }
  }

  return 200;
}

std::future<int> Gateway::AsyncPush() { return async_push(HttpMethod::Post); }

std::future<int> Gateway::AsyncPushAdd() { return async_push(HttpMethod::Put); }

std::future<int> Gateway::async_push(HttpMethod method) {
  const auto serializer = TextSerializer{};
  std::vector<std::future<int>> futures;

  for (auto& wcollectable : collectables_) {
    auto collectable = wcollectable.first.lock();
    if (!collectable) {
      continue;
    }

    auto metrics = collectable->Collect();
    auto body = std::make_shared<std::string>(serializer.Serialize(metrics));
    auto uri = getUri(wcollectable);

    futures.push_back(std::async(std::launch::async, [method, uri, body, this] {
      return performHttpRequest(method, uri, *body);
    }));
  }

  const auto reduceFutures = [](std::vector<std::future<int>> lfutures) {
    auto final_status_code = 200;

    for (auto& future : lfutures) {
      auto status_code = future.get();

      if (status_code < 100 || status_code >= 400) {
        final_status_code = status_code;
      }
    }

    return final_status_code;
  };

  return std::async(std::launch::async, reduceFutures, std::move(futures));
}

int Gateway::Delete() {
  return performHttpRequest(HttpMethod::Delete, jobUri_, {});
}

std::future<int> Gateway::AsyncDelete() {
  return std::async(std::launch::async, [&] { return Delete(); });
}

}  // namespace prometheus

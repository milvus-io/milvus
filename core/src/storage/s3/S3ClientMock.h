// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <memory>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace milvus {
namespace storage {

/*
 * This is a class that represents a S3 Client which is used to mimic the put/get operations of a actual s3 client.
 * During a put object, the body of the request is stored as well as the metadata of the request. This data is then
 * populated into a get object result when a get operation is called.
 */
class S3ClientMock : public Aws::S3::S3Client {
 public:
    explicit S3ClientMock(Aws::Client::ClientConfiguration clientConfiguration = Aws::Client::ClientConfiguration())
        : S3Client(Aws::Auth::AWSCredentials("", ""), clientConfiguration) {
    }

    Aws::S3::Model::CreateBucketOutcome
    CreateBucket(const Aws::S3::Model::CreateBucketRequest& request) const override {
        Aws::S3::Model::CreateBucketResult result;
        return Aws::S3::Model::CreateBucketOutcome(std::move(result));
    }

    Aws::S3::Model::DeleteBucketOutcome
    DeleteBucket(const Aws::S3::Model::DeleteBucketRequest& request) const override {
        Aws::NoResult result;
        return Aws::S3::Model::DeleteBucketOutcome(std::move(result));
    }

    Aws::S3::Model::PutObjectOutcome
    PutObject(const Aws::S3::Model::PutObjectRequest& request) const override {
        Aws::String key = request.GetKey();
        std::shared_ptr<Aws::IOStream> body = request.GetBody();
        aws_map_[key] = body;

        Aws::S3::Model::PutObjectResult result;
        return Aws::S3::Model::PutObjectOutcome(std::move(result));
    }

    Aws::S3::Model::GetObjectOutcome
    GetObject(const Aws::S3::Model::GetObjectRequest& request) const override {
        auto factory = request.GetResponseStreamFactory();
        Aws::Utils::Stream::ResponseStream resp_stream(factory);

        try {
            std::shared_ptr<Aws::IOStream> body = aws_map_.at(request.GetKey());
            Aws::String body_str((Aws::IStreamBufIterator(*body)), Aws::IStreamBufIterator());

            resp_stream.GetUnderlyingStream().write(body_str.c_str(), body_str.length());
            resp_stream.GetUnderlyingStream().flush();
            Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream> awsStream(
                std::move(resp_stream), Aws::Http::HeaderValueCollection());

            Aws::S3::Model::GetObjectResult result(std::move(awsStream));
            return Aws::S3::Model::GetObjectOutcome(std::move(result));
        } catch (...) {
            return Aws::S3::Model::GetObjectOutcome();
        }
    }

    Aws::S3::Model::ListObjectsOutcome
    ListObjects(const Aws::S3::Model::ListObjectsRequest& request) const override {
        /* TODO: add object key list into ListObjectsOutcome */

        Aws::S3::Model::ListObjectsResult result;
        return Aws::S3::Model::ListObjectsOutcome(std::move(result));
    }

    Aws::S3::Model::DeleteObjectOutcome
    DeleteObject(const Aws::S3::Model::DeleteObjectRequest& request) const override {
        Aws::String key = request.GetKey();
        aws_map_.erase(key);
        Aws::S3::Model::DeleteObjectResult result;
        Aws::S3::Model::DeleteObjectOutcome(std::move(result));
        return result;
    }

    mutable Aws::Map<Aws::String, std::shared_ptr<Aws::IOStream>> aws_map_;
};

}  // namespace storage
}  // namespace milvus

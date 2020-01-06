// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <memory>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/S3Client.h>

/*
 * This is a class that represents a S3 Client which is used to mimic the put/get operations of a actual s3 client.
 * During a put object, the body of the request is stored as well as the metadata of the request. This data is then
 * populated into a get object result when a get operation is called.
 */
class MockS3Client : public Aws::S3::S3Client {
 public:
    explicit MockS3Client(Aws::Client::ClientConfiguration clientConfiguration = Aws::Client::ClientConfiguration()) :
        S3Client(Aws::Auth::AWSCredentials("", ""), clientConfiguration) {
    }

    Aws::S3::Model::CreateBucketOutcome CreateBucket(const Aws::S3::Model::CreateBucketRequest&) const override {
        Aws::S3::Model::CreateBucketOutcome outcome;
        Aws::S3::Model::CreateBucketResult result(outcome.GetResultWithOwnership());
        return result;
    }

    Aws::S3::Model::DeleteBucketOutcome DeleteBucket(const Aws::S3::Model::DeleteBucketRequest&) const override {
        Aws::NoResult result;
        return result;
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest& request) const override {
        std::shared_ptr<Aws::IOStream> body = request.GetBody();
        Aws::String tempBodyString((Aws::IStreamBufIterator(*body)), Aws::IStreamBufIterator());
        bodyString = tempBodyString;

        Aws::S3::Model::PutObjectOutcome outcome;
        Aws::S3::Model::PutObjectResult result(outcome.GetResultWithOwnership());
        return result;
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest& request) const override {
        auto factory = request.GetResponseStreamFactory();
        Aws::Utils::Stream::ResponseStream responseStream(factory);

        responseStream.GetUnderlyingStream().write(bodyString.c_str(), bodyString.length());

        responseStream.GetUnderlyingStream().flush();
        Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>
            awsStream(std::move(responseStream), Aws::Http::HeaderValueCollection());
        Aws::S3::Model::GetObjectResult getObjectResult(std::move(awsStream));
        return Aws::S3::Model::GetObjectOutcome(std::move(getObjectResult));
    }

    Aws::S3::Model::ListObjectsOutcome ListObjects(const Aws::S3::Model::ListObjectsRequest&) const override {
        Aws::S3::Model::ListObjectsOutcome outcome;
        Aws::S3::Model::ListObjectsResult result(outcome.GetResultWithOwnership());
        return result;
    }

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(const Aws::S3::Model::DeleteObjectRequest&) const override {
        Aws::S3::Model::DeleteObjectOutcome outcome;
        Aws::S3::Model::DeleteObjectResult result(outcome.GetResultWithOwnership());
        return result;
    }

    mutable Aws::String bodyString;
};


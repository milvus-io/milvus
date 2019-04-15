/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#import "TSocketTransport.h"

#if !TARGET_OS_IPHONE
#import <CoreServices/CoreServices.h>
#else
#import <CFNetwork/CFNetwork.h>
#endif

#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>

@interface TSocketTransport () <NSStreamDelegate>
@end


@implementation TSocketTransport

- (id) initWithReadStream: (CFReadStreamRef) readStream writeStream: (CFWriteStreamRef) writeStream
{
  NSInputStream *inputStream = nil;
  NSOutputStream *outputStream = nil;

  if (readStream && writeStream) {

    CFReadStreamSetProperty(readStream, kCFStreamPropertyShouldCloseNativeSocket, kCFBooleanTrue);
    CFWriteStreamSetProperty(writeStream, kCFStreamPropertyShouldCloseNativeSocket, kCFBooleanTrue);

    inputStream = (__bridge NSInputStream *)readStream;
    [inputStream setDelegate:self];
    [inputStream scheduleInRunLoop:NSRunLoop.currentRunLoop forMode:NSDefaultRunLoopMode];
    [inputStream open];

    outputStream = (__bridge NSOutputStream *)writeStream;
    [outputStream setDelegate:self];
    [outputStream scheduleInRunLoop:NSRunLoop.currentRunLoop forMode:NSDefaultRunLoopMode];
    [outputStream open];
  }
  else {

    if (readStream) {
      CFRelease(readStream);
    }

    if (writeStream) {
      CFRelease(writeStream);
    }

    return nil;
  }

  return [super initWithInputStream:inputStream outputStream:outputStream];
}

- (id) initWithHostname: (NSString *) hostname
                   port: (int) port
{
  CFReadStreamRef readStream = NULL;
  CFWriteStreamRef writeStream = NULL;
  CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, (__bridge CFStringRef)hostname, port, &readStream, &writeStream);
  return [self initWithReadStream:readStream writeStream:writeStream];
}

- (id) initWithPath: (NSString *) path
{
  CFSocketNativeHandle sockfd = socket(AF_LOCAL, SOCK_STREAM, IPPROTO_IP);
  int yes = 1;
  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
  {
    NSLog(@"TSocketTransport: Unable to set REUSEADDR property of socket.");
    return nil;
  }

  NSData *serverAddress = [[self class] createAddressWithPath:path];

  CFReadStreamRef readStream = NULL;
  CFWriteStreamRef writeStream = NULL;
  CFStreamCreatePairWithSocket(kCFAllocatorDefault, sockfd, &readStream, &writeStream);
  if (!readStream || !writeStream)
  {
    NSLog(@"TSocketTransport: Unable to create read/write stream pair for socket.");
    return nil;
  }

  if (connect(sockfd, (struct sockaddr *)serverAddress.bytes, (socklen_t) serverAddress.length) < 0)
  {
    NSLog(@"TSocketTransport: Connect error: %s\n", strerror(errno));
    return nil;
  }

  return [self initWithReadStream:readStream writeStream:writeStream];
}

+ (NSData *) createAddressWithPath: (NSString *)path
{
  struct sockaddr_un servaddr;

  size_t nullTerminatedPathLength = path.length + 1;
  if (nullTerminatedPathLength> sizeof(servaddr.sun_path)) {
    NSLog(@"TSocketTransport: Unable to create socket at path %@. Path is too long.", path);
    return nil;
  }

  bzero(&servaddr,sizeof(servaddr));
  servaddr.sun_family = AF_LOCAL;
  memcpy(servaddr.sun_path, path.UTF8String, nullTerminatedPathLength);
  servaddr.sun_len = SUN_LEN(&servaddr);

  return [NSData dataWithBytes:&servaddr length:sizeof(servaddr)];
}


@end

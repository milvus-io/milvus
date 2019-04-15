#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

use 5.10.0;
use strict;
use warnings;

use Thrift;
use Thrift::SSLSocket;
use Thrift::ServerSocket;

use IO::Socket::SSL;

package Thrift::SSLServerSocket;
use base qw( Thrift::ServerSocket );
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

#
# Constructor.
# Takes a hash:
# See Thrift::Socket for base class parameters.
# @param[in]  ca     certificate authority filename - not required
# @param[in]  cert   certificate filename; may contain key in which case key is not required
# @param[in]  key    private key filename for the certificate if it is not inside the cert file
#
sub new
{
    my $classname = shift;
    my $self      = $classname->SUPER::new(@_);
    return bless($self, $classname);
}

sub __client
{
  return Thrift::SSLSocket->new();
}

sub __listen
{
    my $self = shift;
    my $opts = {Listen        => $self->{queue},
                LocalAddr     => $self->{host},
                LocalPort     => $self->{port},
                Proto         => 'tcp',
                ReuseAddr     => 1};

    my $verify = IO::Socket::SSL::SSL_VERIFY_PEER | IO::Socket::SSL::SSL_VERIFY_FAIL_IF_NO_PEER_CERT | IO::Socket::SSL::SSL_VERIFY_CLIENT_ONCE;

    $opts->{SSL_ca_file}      = $self->{ca}      if defined $self->{ca};
    $opts->{SSL_cert_file}    = $self->{cert}    if defined $self->{cert};
    $opts->{SSL_cipher_list}  = $self->{ciphers} if defined $self->{ciphers};
    $opts->{SSL_key_file}     = $self->{key}     if defined $self->{key};
    $opts->{SSL_use_cert}     = (defined $self->{cert}) ? 1 : 0;
    $opts->{SSL_verify_mode}  = (defined $self->{ca}) ? $verify : IO::Socket::SSL::SSL_VERIFY_NONE;
    $opts->{SSL_version}      = (defined $self->{version}) ? $self->{version} : 'SSLv23:!SSLv3:!SSLv2';

    return IO::Socket::SSL->new(%$opts);
}

1;

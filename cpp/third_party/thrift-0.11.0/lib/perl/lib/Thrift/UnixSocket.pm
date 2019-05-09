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
use Thrift::Socket;

use IO::Socket::UNIX;

package Thrift::UnixSocket;
use base qw( Thrift::Socket );
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

#
# Constructor.
# Takes a unix domain socket filename.
# See Thrift::Socket for base class parameters.
# @param[in]  path   path to unix socket file
# @example    my $sock = new Thrift::UnixSocket($path);
#
sub new
{
    my $classname = shift;
    my $self      = $classname->SUPER::new();
    $self->{path} = shift;
    return bless($self, $classname);
}

sub __open
{
    my $self = shift;

    my $sock = IO::Socket::UNIX->new(
        Type      => IO::Socket::SOCK_STREAM,
        Peer      => $self->{path})
    || do {
        my $error = 'UnixSocket: Could not connect to ' .
            $self->{path} . ' (' . $! . ')';
        if ($self->{debug}) {
            $self->{debugHandler}->($error);
        }
        die new Thrift::TTransportException($error, Thrift::TTransportException::NOT_OPEN);
    };

    return $sock;
}

1;

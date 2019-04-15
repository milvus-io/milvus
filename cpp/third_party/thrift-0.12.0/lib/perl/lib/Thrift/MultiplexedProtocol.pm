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
use Thrift::MessageType;
use Thrift::Protocol;
use Thrift::ProtocolDecorator;

package Thrift::MultiplexedProtocol;
use base qw(Thrift::ProtocolDecorator);
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

use constant SEPARATOR  => ':';

sub new {
    my $classname = shift;
    my $protocol  = shift;
    my $serviceName  = shift;
    my $self      = $classname->SUPER::new($protocol);

    $self->{serviceName} = $serviceName;

    return bless($self,$classname);
}

#
# Writes the message header.
# Prepends the service name to the function name, separated by MultiplexedProtocol::SEPARATOR.
#
# @param string $name  Function name.
# @param int    $type  Message type.
# @param int    $seqid The sequence id of this message.
#
sub writeMessageBegin
{
    my $self = shift;
    my ($name, $type, $seqid) = @_;

    if ($type == Thrift::TMessageType::CALL || $type == Thrift::TMessageType::ONEWAY) {
        my $nameWithService = $self->{serviceName}.SEPARATOR.$name;
        $self->SUPER::writeMessageBegin($nameWithService, $type, $seqid);
    }
    else {
        $self->SUPER::writeMessageBegin($name, $type, $seqid);
    }
}

1;

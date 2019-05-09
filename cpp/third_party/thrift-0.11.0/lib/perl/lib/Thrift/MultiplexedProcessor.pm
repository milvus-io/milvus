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
use Thrift::MultiplexedProtocol;
use Thrift::Protocol;
use Thrift::ProtocolDecorator;

package Thrift::StoredMessageProtocol;
use base qw(Thrift::ProtocolDecorator);
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

sub new {
    my $classname = shift;
    my $protocol  = shift;
    my $fname  = shift;
    my $mtype  = shift;
    my $rseqid  = shift;
    my $self  = $classname->SUPER::new($protocol);

    $self->{fname} = $fname;
    $self->{mtype} = $mtype;
    $self->{rseqid} = $rseqid;

    return bless($self,$classname);
}

sub readMessageBegin
{
    my $self = shift;
    my $name = shift;
    my $type = shift;
    my $seqid = shift;

    $$name = $self->{fname};
    $$type = $self->{mtype};
    $$seqid = $self->{rseqid};
}

package Thrift::MultiplexedProcessor;
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

sub new {
    my $classname = shift;
    my $self      = {};

    $self->{serviceProcessorMap} = {};
    $self->{defaultProcessor} = undef;

    return bless($self,$classname);
}

sub defaultProcessor {
    my $self = shift;
    my $processor = shift;

    $self->{defaultProcessor} = $processor;
}

sub registerProcessor {
    my $self = shift;
    my $serviceName = shift;
    my $processor = shift;

     $self->{serviceProcessorMap}->{$serviceName} = $processor;
}

sub process {
    my $self = shift;
    my $input = shift;
    my $output = shift;

    #
    #  Use the actual underlying protocol (e.g. BinaryProtocol) to read the
    #  message header. This pulls the message "off the wire", which we'll
    #  deal with at the end of this method.
    #

    my ($fname, $mtype, $rseqid);
    $input->readMessageBegin(\$fname, \$mtype, \$rseqid);

    if ($mtype ne Thrift::TMessageType::CALL && $mtype ne Thrift::TMessageType::ONEWAY) {
        die new Thrift::TException("This should not have happened!?");
    }

    # Extract the service name and the new Message name.
    if (index($fname, Thrift::MultiplexedProtocol::SEPARATOR) == -1) {
        if (defined $self->{defaultProcessor}) {
            return $self->{defaultProcessor}->process(
                new Thrift::StoredMessageProtocol($input, $fname, $mtype, $rseqid), $output
            );
        } else {
            die new Thrift::TException("Service name not found in message name: {$fname} and no default processor defined. Did you " .
                "forget to use a MultiplexProtocol in your client?");
        }
    }

    (my $serviceName, my $messageName) = split(':', $fname, 2);

    if (!exists($self->{serviceProcessorMap}->{$serviceName})) {
        die new Thrift::TException("Service name not found: {$serviceName}.  Did you forget " .
            "to call registerProcessor()?");
    }

    # Dispatch processing to the stored processor
    my $processor = $self->{serviceProcessorMap}->{$serviceName};
    return $processor->process(
        new Thrift::StoredMessageProtocol($input, $messageName, $mtype, $rseqid), $output
    );
}

1;

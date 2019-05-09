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

use HTTP::Request;
use IO::String;
use LWP::UserAgent;
use Thrift;
use Thrift::Exception;
use Thrift::Transport;

package Thrift::HttpClient;
use base('Thrift::Transport');
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

sub new
{
    my $classname = shift;
    my $url       = shift || 'http://localhost:9090';

    my $out = IO::String->new;
    binmode($out);

    my $self = {
        url          => $url,
        out          => $out,
        timeout      => 100,
        handle       => undef,
        headers      => {},
    };

    return bless($self,$classname);
}

sub setTimeout
{
    my $self    = shift;
    my $timeout = shift;

    $self->{timeout} = $timeout;
}

sub setRecvTimeout
{
    warn "setRecvTimeout is deprecated - use setTimeout instead";
    # note: recvTimeout was never used so we do not need to do anything here
}

sub setSendTimeout
{
    my $self    = shift;
    my $timeout = shift;

    warn "setSendTimeout is deprecated - use setTimeout instead";

    $self->setTimeout($timeout);
}

sub setHeader
{
    my $self = shift;
    my ($name, $value) = @_;

    $self->{headers}->{$name} = $value;
}

#
# Tests whether this is open
#
# @return bool true if the socket is open
#
sub isOpen
{
    return 1;
}

sub open {}

#
# Cleans up the buffer.
#
sub close
{
    my $self = shift;
    if (defined($self->{io})) {
      close($self->{io});
      $self->{io} = undef;
    }
}

#
# Guarantees that the full amount of data is read.
#
# @return string The data, of exact length
# @throws TTransportException if cannot read data
#
sub readAll
{
    my $self = shift;
    my $len  = shift;

    my $buf = $self->read($len);

    if (!defined($buf)) {
      die new Thrift::TTransportException("TSocket: Could not read $len bytes from input buffer",
                                          Thrift::TTransportException::END_OF_FILE);
    }
    return $buf;
}

#
# Read and return string
#
sub read
{
    my $self = shift;
    my $len  = shift;

    my $buf;

    my $in = $self->{in};

    if (!defined($in)) {
      die new Thrift::TTransportException("Response buffer is empty, no request.",
                                          Thrift::TTransportException::END_OF_FILE);
    }
    eval {
      my $ret = sysread($in, $buf, $len);
      if (! defined($ret)) {
        die new Thrift::TTransportException("No more data available.",
                                            Thrift::TTransportException::TIMED_OUT);
      }
    }; if($@){
      die new Thrift::TTransportException("$@", Thrift::TTransportException::UNKNOWN);
    }

    return $buf;
}

#
# Write string
#
sub write
{
    my $self = shift;
    my $buf  = shift;
    $self->{out}->print($buf);
}

#
# Flush output (do the actual HTTP/HTTPS request)
#
sub flush
{
    my $self = shift;

    my $ua = LWP::UserAgent->new('timeout' => ($self->{timeout} / 1000),
      'agent' => 'Perl/THttpClient'
     );
    $ua->default_header('Accept' => 'application/x-thrift');
    $ua->default_header('Content-Type' => 'application/x-thrift');
    $ua->cookie_jar({}); # hash to remember cookies between redirects

    my $out = $self->{out};
    $out->setpos(0); # rewind
    my $buf = join('', <$out>);

    my $request = new HTTP::Request(POST => $self->{url}, undef, $buf);
    map { $request->header($_ => $self->{headers}->{$_}) } keys %{$self->{headers}};
    my $response = $ua->request($request);
    my $content_ref = $response->content_ref;

    my $in = IO::String->new($content_ref);
    binmode($in);
    $self->{in} = $in;
    $in->setpos(0); # rewind

    # reset write buffer
    $out = IO::String->new;
    binmode($out);
    $self->{out} = $out;
}

1;

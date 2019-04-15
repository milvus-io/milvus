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

use IO::Socket::SSL;

package Thrift::SSLSocket;
use base qw( Thrift::Socket );
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

#
# Construction and usage
#
# my $opts = {}
# my $socket = Thrift::SSLSocket->new(\%opts);
#
# options:
#
# Any option from Socket.pm is valid, and then:
#
# ca          => certificate authority file (PEM file) to authenticate the
#                server against; if not specified then the server is not
#                authenticated
# cert        => certificate to use as the client; if not specified then
#                the client does not present one but still connects using
#                secure protocol
# ciphers     => allowed cipher list
#                (see http://www.openssl.org/docs/apps/ciphers.html#CIPHER_STRINGS)
# key         => certificate key for "cert" option
# version     => acceptable SSL/TLS versions - if not specified then the
#                default is to use SSLv23 handshake but only negotiate
#                at TLSv1.0 or later
#

sub new
{
    my $classname = shift;
    my $self      = $classname->SUPER::new(@_);

    return bless($self, $classname);
}

sub __open
{
    my $self = shift;
    my $opts = {PeerAddr      => $self->{host},
                PeerPort      => $self->{port},
                Proto         => 'tcp',
                Timeout       => $self->{sendTimeout} / 1000};

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

sub __close
{
    my $self = shift;
    my $sock = ($self->{handle}->handles())[0];
    if ($sock) {
      $sock->close(SSL_no_shutdown => 1);
    }
}

sub __recv
{
  my $self = shift;
  my $sock = shift;
  my $len = shift;
  my $buf = undef;
  if ($sock) {
    sysread($sock, $buf, $len);
  }
  return $buf;
}

sub __send
{
    my $self = shift;
    my $sock = shift;
    my $buf = shift;
    return syswrite($sock, $buf);
}

sub __wait
{
    my $self = shift;
    my $sock = ($self->{handle}->handles())[0];
    if ($sock and $sock->pending() eq 0) {
        return $self->SUPER::__wait();
    }
    return $sock;
}


1;

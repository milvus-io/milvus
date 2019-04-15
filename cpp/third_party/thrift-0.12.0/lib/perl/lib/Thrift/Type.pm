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

#
# Data types that can be sent via Thrift
#
package Thrift::TType;
use version 0.77; our $VERSION = version->declare("$Thrift::VERSION");

use constant STOP   => 0;
use constant VOID   => 1;
use constant BOOL   => 2;
use constant BYTE   => 3;
use constant I08    => 3;
use constant DOUBLE => 4;
use constant I16    => 6;
use constant I32    => 8;
use constant I64    => 10;
use constant STRING => 11;
use constant UTF7   => 11;
use constant STRUCT => 12;
use constant MAP    => 13;
use constant SET    => 14;
use constant LIST   => 15;
use constant UTF8   => 16;
use constant UTF16  => 17;

1;

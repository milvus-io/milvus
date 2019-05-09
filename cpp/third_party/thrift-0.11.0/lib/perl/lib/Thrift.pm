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

#
# Versioning
#
# Every perl module for Thrift will have the same version
# declaration.  For a production build, change it below to
# something like "v0.11.0" and all of the packages in all
# of the files will pick it up from here.
#

package Thrift;
use version 0.77; our $VERSION = version->declare("v0.11.0");

1;

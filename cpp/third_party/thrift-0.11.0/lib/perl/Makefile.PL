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

use ExtUtils::MakeMaker;
WriteMakefile( ABSTRACT => 'Apache Thrift is a software framework for scalable cross-language services development.',
               AUTHOR => 'Apache Thrift <dev@thrift.apache.org>',
               LICENSE => 'apache_2_0',
               MIN_PERL_VERSION => '5.010000',
               NAME => 'Thrift',
               NEEDS_LINKING => 0,
               PREREQ_PM => {
                   'Bit::Vector'     => 0,
                   'Class::Accessor' => 0
               },
               VERSION_FROM => 'lib/Thrift.pm' );

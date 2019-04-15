<?php
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
 *
 * @package thrift.processor
 */

namespace Thrift;

use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TProtocolDecorator;

/**
 *  Our goal was to work with any protocol. In order to do that, we needed
 *  to allow them to call readMessageBegin() and get the Message in exactly
 *  the standard format, without the service name prepended to the Message name.
 */
class StoredMessageProtocol extends TProtocolDecorator
{
    private $fname_;
    private $mtype_;
    private $rseqid_;

    public function __construct(TProtocol $protocol, $fname, $mtype, $rseqid)
    {
        parent::__construct($protocol);
        $this->fname_  = $fname;
        $this->mtype_  = $mtype;
        $this->rseqid_ = $rseqid;
    }

    public function readMessageBegin(&$name, &$type, &$seqid)
    {
        $name  = $this->fname_;
        $type  = $this->mtype_;
        $seqid = $this->rseqid_;
    }
}

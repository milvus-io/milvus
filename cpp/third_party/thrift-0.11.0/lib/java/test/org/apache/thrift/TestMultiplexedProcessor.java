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
 */

package org.apache.thrift;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Before;
import org.junit.Test;

public class TestMultiplexedProcessor {
  private TMultiplexedProcessor mp;
  private TProtocol iprot;
  private TProtocol oprot;

  @Before
  public void setUp() throws Exception {
    mp = new TMultiplexedProcessor();
    iprot = mock(TProtocol.class);
    oprot = mock(TProtocol.class);
  }

  @Test(expected = TException.class)
  public void testWrongMessageType() throws TException {
    when (iprot.readMessageBegin()).thenReturn(new TMessage("service:func", TMessageType.REPLY, 42));
    mp.process(iprot, oprot);
  }

  @Test(expected = TException.class)
  public void testNoSuchService() throws TException {
    when(iprot.readMessageBegin()).thenReturn(new TMessage("service:func", TMessageType.CALL, 42));

    mp.process(iprot, oprot);
  }

  static class StubProcessor implements TProcessor {
    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      TMessage msg = in.readMessageBegin();
      if (!"func".equals(msg.name) || msg.type!=TMessageType.CALL || msg.seqid!=42) {
        throw new TException("incorrect parameters");
      }
      out.writeMessageBegin(new TMessage("func", TMessageType.REPLY, 42));
      return true;
    }
  }

  @Test
  public void testExistingService() throws TException {
    when(iprot.readMessageBegin()).thenReturn(new TMessage("service:func", TMessageType.CALL, 42));
    mp.registerProcessor("service", new StubProcessor());
    mp.process(iprot, oprot);
    verify(oprot).writeMessageBegin(any(TMessage.class));
  }

  @Test
  public void testDefaultService() throws TException {
    when(iprot.readMessageBegin()).thenReturn(new TMessage("func", TMessageType.CALL, 42));
    mp.registerDefault(new StubProcessor());
    mp.process(iprot, oprot);
    verify(oprot).writeMessageBegin(any(TMessage.class));
  }

}

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

package org.apache.thrift.transport;

import java.io.FileInputStream;
import java.net.InetAddress;

public class TestTSSLTransportFactoryStreamedStore extends TestTSSLTransportFactory {
  private static String keyStoreLocation = System.getProperty("javax.net.ssl.keyStore");
  private static String trustStoreLocation = System.getProperty("javax.net.ssl.trustStore");
  
  public TestTSSLTransportFactoryStreamedStore() {
    super();
    
    /**
     *  Override system properties to be able to test passing
     *  the trustStore and keyStore as input stream
     */
    System.setProperty("javax.net.ssl.trustStore", "");
    System.setProperty("javax.net.ssl.keyStore", "");
  }

  @Override
  public TTransport getClientTransport(TTransport underlyingTransport)
  throws Exception {
    TSSLTransportFactory.TSSLTransportParameters params = new
      TSSLTransportFactory.TSSLTransportParameters();

    params.setTrustStore(new FileInputStream(trustStoreLocation),
                         System.getProperty("javax.net.ssl.trustStorePassword"));
    
    return TSSLTransportFactory.getClientSocket(HOST, PORT, 0/*timeout*/, params);
  }

  @Override
  protected TServerSocket getServerTransport() throws Exception {
    TSSLTransportFactory.TSSLTransportParameters params = new
        TSSLTransportFactory.TSSLTransportParameters();
    
    params.setKeyStore(new FileInputStream(keyStoreLocation), 
                       System.getProperty("javax.net.ssl.keyStorePassword"));
    
    return TSSLTransportFactory.getServerSocket(PORT, 0/*timeout*/, InetAddress.getByName(HOST), params);
  }
}
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

import junit.framework.TestCase;
import thrift.test.enumcontainers.EnumContainersTestConstants;
import thrift.test.enumcontainers.GodBean;
import thrift.test.enumcontainers.GreekGodGoddess;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;

public class TestEnumContainers extends TestCase {

    public void testEnumContainers() throws Exception {
        final GodBean b1 = new GodBean();
        b1.addToGoddess(GreekGodGoddess.HERA);
        b1.getGoddess().add(GreekGodGoddess.APHRODITE);
        b1.putToPower(GreekGodGoddess.ZEUS, 1000);
        b1.getPower().put(GreekGodGoddess.HERA, 333);
        b1.putToByAlias("Mr. Z", GreekGodGoddess.ZEUS);
        b1.addToImages("Baths of Aphrodite 01.jpeg");

        final GodBean b2 = new GodBean(b1);

        final GodBean b3 = new GodBean();
        {
            final TSerializer serializer = new TSerializer();
            final TDeserializer deserializer = new TDeserializer();

            final byte[] bytes = serializer.serialize(b1);
            deserializer.deserialize(b3, bytes);
        }

        assertTrue(b1.getGoddess() != b2.getGoddess());
        assertTrue(b1.getPower() != b2.getPower());

        assertTrue(b1.getGoddess() != b3.getGoddess());
        assertTrue(b1.getPower() != b3.getPower());

        for (GodBean each : new GodBean[]{b1, b2, b3}) {
            assertTrue(each.getGoddess().contains(GreekGodGoddess.HERA));
            assertFalse(each.getGoddess().contains(GreekGodGoddess.POSEIDON));
            assertTrue(each.getGoddess() instanceof EnumSet);

            assertEquals(Integer.valueOf(1000), each.getPower().get(GreekGodGoddess.ZEUS));
            assertEquals(Integer.valueOf(333), each.getPower().get(GreekGodGoddess.HERA));
            assertTrue(each.getPower() instanceof EnumMap);

            assertTrue(each.getByAlias() instanceof HashMap);
            assertTrue(each.getImages() instanceof HashSet);
        }
    }

    public void testEnumConstants() {
        assertEquals("lightning bolt", EnumContainersTestConstants.ATTRIBUTES.get(GreekGodGoddess.ZEUS));
        assertTrue(EnumContainersTestConstants.ATTRIBUTES instanceof EnumMap);

        assertTrue(EnumContainersTestConstants.BEAUTY.contains(GreekGodGoddess.APHRODITE));
        assertTrue(EnumContainersTestConstants.BEAUTY instanceof EnumSet);
    }
}

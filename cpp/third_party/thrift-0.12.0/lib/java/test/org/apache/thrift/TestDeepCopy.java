package org.apache.thrift;

import junit.framework.TestCase;
import thrift.test.DeepCopyBar;
import thrift.test.DeepCopyFoo;

public class TestDeepCopy extends TestCase {

  public void testDeepCopy() throws Exception {
    final DeepCopyFoo foo = new DeepCopyFoo();

    foo.addToL(new DeepCopyBar());
    foo.addToS(new DeepCopyBar());
    foo.putToM("test 3", new DeepCopyBar());

    foo.addToLi(new thrift.test.Object());
    foo.addToSi(new thrift.test.Object());
    foo.putToMi("test 3", new thrift.test.Object());

    foo.setBar(new DeepCopyBar());

    final DeepCopyFoo deepCopyFoo = foo.deepCopy();

    assertNotSame(foo.getBar(), deepCopyFoo.getBar());

    assertNotSame(foo.getL().get(0),                          deepCopyFoo.getL().get(0));
    assertNotSame(foo.getS().toArray(new DeepCopyBar[0])[0],  deepCopyFoo.getS().toArray(new DeepCopyBar[0])[0]);
    assertNotSame(foo.getM().get("test 3"),                   deepCopyFoo.getM().get("test 3"));

    assertNotSame(foo.getLi().get(0),                                 deepCopyFoo.getLi().get(0));
    assertNotSame(foo.getSi().toArray(new thrift.test.Object[0])[0],  deepCopyFoo.getSi().toArray(new thrift.test.Object[0])[0]);
    assertNotSame(foo.getMi().get("test 3"),                          deepCopyFoo.getMi().get("test 3"));
  }
}

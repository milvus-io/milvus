// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb.test;

import org.junit.internal.JUnitSystem;
import org.junit.internal.RealSystem;
import org.junit.internal.TextListener;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.rocksdb.RocksDB;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import static org.rocksdb.test.RocksJunitRunner.RocksJunitListener.Status.*;

/**
 * Custom Junit Runner to print also Test classes
 * and executed methods to command prompt.
 */
public class RocksJunitRunner {

  /**
   * Listener which overrides default functionality
   * to print class and method to system out.
   */
  static class RocksJunitListener extends TextListener {

    private final static NumberFormat secsFormat =
        new DecimalFormat("###,###.###");

    private final PrintStream writer;

    private String currentClassName = null;
    private String currentMethodName = null;
    private Status currentStatus = null;
    private long currentTestsStartTime;
    private int currentTestsCount = 0;
    private int currentTestsIgnoredCount = 0;
    private int currentTestsFailureCount = 0;
    private int currentTestsErrorCount = 0;

    enum Status {
      IGNORED,
      FAILURE,
      ERROR,
      OK
    }

    /**
     * RocksJunitListener constructor
     *
     * @param system JUnitSystem
     */
    public RocksJunitListener(final JUnitSystem system) {
      this(system.out());
    }

    public RocksJunitListener(final PrintStream writer) {
      super(writer);
      this.writer = writer;
    }

    @Override
    public void testRunStarted(final Description description) {
      writer.format("Starting RocksJava Tests...%n");

    }

    @Override
    public void testStarted(final Description description) {
      if(currentClassName == null
          || !currentClassName.equals(description.getClassName())) {
        if(currentClassName !=  null) {
          printTestsSummary();
        } else {
          currentTestsStartTime = System.currentTimeMillis();
        }
        writer.format("%nRunning: %s%n", description.getClassName());
        currentClassName = description.getClassName();
      }
      currentMethodName = description.getMethodName();
      currentStatus = OK;
      currentTestsCount++;
    }

    private void printTestsSummary() {
      // print summary of last test set
      writer.format("Tests run: %d, Failures: %d, Errors: %d, Ignored: %d, Time elapsed: %s sec%n",
          currentTestsCount,
          currentTestsFailureCount,
          currentTestsErrorCount,
          currentTestsIgnoredCount,
          formatSecs(System.currentTimeMillis() - currentTestsStartTime));

      // reset counters
      currentTestsCount = 0;
      currentTestsFailureCount = 0;
      currentTestsErrorCount = 0;
      currentTestsIgnoredCount = 0;
      currentTestsStartTime = System.currentTimeMillis();
    }

    private static String formatSecs(final double milliseconds) {
      final double seconds = milliseconds / 1000;
      return secsFormat.format(seconds);
    }

    @Override
    public void testFailure(final Failure failure) {
      if (failure.getException() != null
          && failure.getException() instanceof AssertionError) {
        currentStatus = FAILURE;
        currentTestsFailureCount++;
      } else {
        currentStatus = ERROR;
        currentTestsErrorCount++;
      }
    }

    @Override
    public void testIgnored(final Description description) {
      currentStatus = IGNORED;
      currentTestsIgnoredCount++;
    }

    @Override
    public void testFinished(final Description description) {
      if(currentStatus == OK) {
        writer.format("\t%s OK%n",currentMethodName);
      } else {
        writer.format("  [%s] %s%n", currentStatus.name(), currentMethodName);
      }
    }

    @Override
    public void testRunFinished(final Result result) {
      printTestsSummary();
      super.testRunFinished(result);
    }
  }

  /**
   * Main method to execute tests
   *
   * @param args Test classes as String names
   */
  public static void main(final String[] args){
    final JUnitCore runner = new JUnitCore();
    final JUnitSystem system = new RealSystem();
    runner.addListener(new RocksJunitListener(system));
    try {
      final List<Class<?>> classes = new ArrayList<>();
      for (final String arg : args) {
        classes.add(Class.forName(arg));
      }
      final Class[] clazzes = classes.toArray(new Class[classes.size()]);
      final Result result = runner.run(clazzes);
      if(!result.wasSuccessful()) {
        System.exit(-1);
      }
    } catch (final ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(-2);
    }
  }
}

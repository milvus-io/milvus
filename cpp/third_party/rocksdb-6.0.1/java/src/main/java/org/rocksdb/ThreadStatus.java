// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Map;

public class ThreadStatus {
  private final long threadId;
  private final ThreadType threadType;
  private final String dbName;
  private final String cfName;
  private final OperationType operationType;
  private final long operationElapsedTime; // microseconds
  private final OperationStage operationStage;
  private final long  operationProperties[];
  private final StateType stateType;

  /**
   * Invoked from C++ via JNI
   */
  private ThreadStatus(final long threadId,
                       final byte threadTypeValue,
                       final String dbName,
                       final String cfName,
                       final byte operationTypeValue,
                       final long operationElapsedTime,
                       final byte operationStageValue,
                       final long[] operationProperties,
                       final byte stateTypeValue) {
    this.threadId = threadId;
    this.threadType = ThreadType.fromValue(threadTypeValue);
    this.dbName = dbName;
    this.cfName = cfName;
    this.operationType = OperationType.fromValue(operationTypeValue);
    this.operationElapsedTime = operationElapsedTime;
    this.operationStage = OperationStage.fromValue(operationStageValue);
    this.operationProperties = operationProperties;
    this.stateType = StateType.fromValue(stateTypeValue);
  }

  /**
   * Get the unique ID of the thread.
   *
   * @return the thread id
   */
  public long getThreadId() {
    return threadId;
  }

  /**
   * Get the type of the thread.
   *
   * @return the type of the thread.
   */
  public ThreadType getThreadType() {
    return threadType;
  }

  /**
   * The name of the DB instance that the thread is currently
   * involved with.
   *
   * @return the name of the db, or null if the thread is not involved
   *     in any DB operation.
   */
  /* @Nullable */ public String getDbName() {
    return dbName;
  }

  /**
   * The name of the Column Family that the thread is currently
   * involved with.
   *
   * @return the name of the db, or null if the thread is not involved
   *     in any column Family operation.
   */
  /* @Nullable */ public String getCfName() {
    return cfName;
  }

  /**
   * Get the operation (high-level action) that the current thread is involved
   * with.
   *
   * @return the operation
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * Get the elapsed time of the current thread operation in microseconds.
   *
   * @return the elapsed time
   */
  public long getOperationElapsedTime() {
    return operationElapsedTime;
  }

  /**
   * Get the current stage where the thread is involved in the current
   * operation.
   *
   * @return the current stage of the current operation
   */
  public OperationStage getOperationStage() {
    return operationStage;
  }

  /**
   * Get the list of properties that describe some details about the current
   * operation.
   *
   * Each field in might have different meanings for different operations.
   *
   * @return the properties
   */
  public long[] getOperationProperties() {
    return operationProperties;
  }

  /**
   * Get the state (lower-level action) that the current thread is involved
   * with.
   *
   * @return the state
   */
  public StateType getStateType() {
    return stateType;
  }

  /**
   * Get the name of the thread type.
   *
   * @param threadType the thread type
   *
   * @return the name of the thread type.
   */
  public static String getThreadTypeName(final ThreadType threadType) {
    return getThreadTypeName(threadType.getValue());
  }

  /**
   * Get the name of an operation given its type.
   *
   * @param operationType the type of operation.
   *
   * @return the name of the operation.
   */
  public static String getOperationName(final OperationType operationType) {
    return getOperationName(operationType.getValue());
  }

  public static String microsToString(final long operationElapsedTime) {
    return microsToStringNative(operationElapsedTime);
  }

  /**
   * Obtain a human-readable string describing the specified operation stage.
   *
   * @param operationStage the stage of the operation.
   *
   * @return the description of the operation stage.
   */
  public static String getOperationStageName(
      final OperationStage operationStage) {
    return getOperationStageName(operationStage.getValue());
  }

  /**
   * Obtain the name of the "i"th operation property of the
   * specified operation.
   *
   * @param operationType the operation type.
   * @param i the index of the operation property.
   *
   * @return the name of the operation property
   */
  public static String getOperationPropertyName(
      final OperationType operationType, final int i) {
    return getOperationPropertyName(operationType.getValue(), i);
  }

  /**
   * Translate the "i"th property of the specified operation given
   * a property value.
   *
   * @param operationType the operation type.
   * @param operationProperties the operation properties.
   *
   * @return the property values.
   */
  public static Map<String, Long> interpretOperationProperties(
      final OperationType operationType, final long[] operationProperties) {
    return interpretOperationProperties(operationType.getValue(),
        operationProperties);
  }

  /**
   * Obtain the name of a state given its type.
   *
   * @param stateType the state type.
   *
   * @return the name of the state.
   */
  public static String getStateName(final StateType stateType) {
    return getStateName(stateType.getValue());
  }

  private static native String getThreadTypeName(final byte threadTypeValue);
  private static native String getOperationName(final byte operationTypeValue);
  private static native String microsToStringNative(
      final long operationElapsedTime);
  private static native String getOperationStageName(
      final byte operationStageTypeValue);
  private static native String getOperationPropertyName(
      final byte operationTypeValue, final int i);
  private static native Map<String, Long>interpretOperationProperties(
      final byte operationTypeValue, final long[] operationProperties);
  private static native String getStateName(final byte stateTypeValue);
}

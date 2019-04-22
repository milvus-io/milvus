package org.rocksdb;

import static org.rocksdb.AbstractMutableOptions.INT_ARRAY_INT_SEPARATOR;

public abstract class MutableOptionValue<T> {

  abstract double asDouble() throws NumberFormatException;
  abstract long asLong() throws NumberFormatException;
  abstract int asInt() throws NumberFormatException;
  abstract boolean asBoolean() throws IllegalStateException;
  abstract int[] asIntArray() throws IllegalStateException;
  abstract String asString();
  abstract T asObject();

  private static abstract class MutableOptionValueObject<T>
      extends MutableOptionValue<T> {
    protected final T value;

    private MutableOptionValueObject(final T value) {
      this.value = value;
    }

    @Override T asObject() {
      return value;
    }
  }

  static MutableOptionValue<String> fromString(final String s) {
    return new MutableOptionStringValue(s);
  }

  static MutableOptionValue<Double> fromDouble(final double d) {
    return new MutableOptionDoubleValue(d);
  }

  static MutableOptionValue<Long> fromLong(final long d) {
    return new MutableOptionLongValue(d);
  }

  static MutableOptionValue<Integer> fromInt(final int i) {
    return new MutableOptionIntValue(i);
  }

  static MutableOptionValue<Boolean> fromBoolean(final boolean b) {
    return new MutableOptionBooleanValue(b);
  }

  static MutableOptionValue<int[]> fromIntArray(final int[] ix) {
    return new MutableOptionIntArrayValue(ix);
  }

  static <N extends Enum<N>> MutableOptionValue<N> fromEnum(final N value) {
    return new MutableOptionEnumValue<>(value);
  }

  static class MutableOptionStringValue
      extends MutableOptionValueObject<String> {
    MutableOptionStringValue(final String value) {
      super(value);
    }

    @Override
    double asDouble() throws NumberFormatException {
      return Double.parseDouble(value);
    }

    @Override
    long asLong() throws NumberFormatException {
      return Long.parseLong(value);
    }

    @Override
    int asInt() throws NumberFormatException {
      return Integer.parseInt(value);
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      return Boolean.parseBoolean(value);
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new IllegalStateException("String is not applicable as int[]");
    }

    @Override
    String asString() {
      return value;
    }
  }

  static class MutableOptionDoubleValue
      extends MutableOptionValue<Double> {
    private final double value;
    MutableOptionDoubleValue(final double value) {
      this.value = value;
    }

    @Override
    double asDouble() {
      return value;
    }

    @Override
    long asLong() throws NumberFormatException {
      return Double.valueOf(value).longValue();
    }

    @Override
    int asInt() throws NumberFormatException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "double value lies outside the bounds of int");
      }
      return Double.valueOf(value).intValue();
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException(
          "double is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "double value lies outside the bounds of int");
      }
      return new int[] { Double.valueOf(value).intValue() };
    }

    @Override
    String asString() {
      return String.valueOf(value);
    }

    @Override
    Double asObject() {
      return value;
    }
  }

  static class MutableOptionLongValue
      extends MutableOptionValue<Long> {
    private final long value;

    MutableOptionLongValue(final long value) {
      this.value = value;
    }

    @Override
    double asDouble() {
      if(value > Double.MAX_VALUE || value < Double.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return Long.valueOf(value).doubleValue();
    }

    @Override
    long asLong() throws NumberFormatException {
      return value;
    }

    @Override
    int asInt() throws NumberFormatException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return Long.valueOf(value).intValue();
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException(
          "long is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return new int[] { Long.valueOf(value).intValue() };
    }

    @Override
    String asString() {
      return String.valueOf(value);
    }

    @Override
    Long asObject() {
      return value;
    }
  }

  static class MutableOptionIntValue
      extends MutableOptionValue<Integer> {
    private final int value;

    MutableOptionIntValue(final int value) {
      this.value = value;
    }

    @Override
    double asDouble() {
      if(value > Double.MAX_VALUE || value < Double.MIN_VALUE) {
        throw new NumberFormatException("int value lies outside the bounds of int");
      }
      return Integer.valueOf(value).doubleValue();
    }

    @Override
    long asLong() throws NumberFormatException {
      return value;
    }

    @Override
    int asInt() throws NumberFormatException {
      return value;
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException("int is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      return new int[] { value };
    }

    @Override
    String asString() {
      return String.valueOf(value);
    }

    @Override
    Integer asObject() {
      return value;
    }
  }

  static class MutableOptionBooleanValue
      extends MutableOptionValue<Boolean> {
    private final boolean value;

    MutableOptionBooleanValue(final boolean value) {
      this.value = value;
    }

    @Override
    double asDouble() {
      throw new NumberFormatException("boolean is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("boolean is not applicable as Long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("boolean is not applicable as int");
    }

    @Override
    boolean asBoolean() {
      return value;
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new IllegalStateException("boolean is not applicable as int[]");
    }

    @Override
    String asString() {
      return String.valueOf(value);
    }

    @Override
    Boolean asObject() {
      return value;
    }
  }

  static class MutableOptionIntArrayValue
      extends MutableOptionValueObject<int[]> {
    MutableOptionIntArrayValue(final int[] value) {
      super(value);
    }

    @Override
    double asDouble() {
      throw new NumberFormatException("int[] is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("int[] is not applicable as Long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("int[] is not applicable as int");
    }

    @Override
    boolean asBoolean() {
      throw new NumberFormatException("int[] is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      return value;
    }

    @Override
    String asString() {
      final StringBuilder builder = new StringBuilder();
      for(int i = 0; i < value.length; i++) {
        builder.append(i);
        if(i + 1 < value.length) {
          builder.append(INT_ARRAY_INT_SEPARATOR);
        }
      }
      return builder.toString();
    }
  }

  static class MutableOptionEnumValue<T extends Enum<T>>
      extends MutableOptionValueObject<T> {

    MutableOptionEnumValue(final T value) {
      super(value);
    }

    @Override
    double asDouble() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as int");
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new NumberFormatException("Enum is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new NumberFormatException("Enum is not applicable as int[]");
    }

    @Override
    String asString() {
      return value.name();
    }
  }

}

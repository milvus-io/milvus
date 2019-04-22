package org.rocksdb;

import java.util.*;

public abstract class AbstractMutableOptions {

  protected static final String KEY_VALUE_PAIR_SEPARATOR = ";";
  protected static final char KEY_VALUE_SEPARATOR = '=';
  static final String INT_ARRAY_INT_SEPARATOR = ",";

  protected final String[] keys;
  private final String[] values;

  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   */
  protected AbstractMutableOptions(final String[] keys, final String[] values) {
    this.keys = keys;
    this.values = values;
  }

  String[] getKeys() {
    return keys;
  }

  String[] getValues() {
    return values;
  }

  /**
   * Returns a string representation of MutableOptions which
   * is suitable for consumption by {@code #parse(String)}.
   *
   * @return String representation of MutableOptions
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    for(int i = 0; i < keys.length; i++) {
      buffer
          .append(keys[i])
          .append(KEY_VALUE_SEPARATOR)
          .append(values[i]);

      if(i + 1 < keys.length) {
        buffer.append(KEY_VALUE_PAIR_SEPARATOR);
      }
    }
    return buffer.toString();
  }

  public static abstract class AbstractMutableOptionsBuilder<
      T extends AbstractMutableOptions,
      U extends AbstractMutableOptionsBuilder<T, U, K>,
      K extends MutableOptionKey> {

    private final Map<K, MutableOptionValue<?>> options = new LinkedHashMap<>();

    protected abstract U self();

    /**
     * Get all of the possible keys
     *
     * @return A map of all keys, indexed by name.
     */
    protected abstract Map<String, K> allKeys();

    /**
     * Construct a sub-class instance of {@link AbstractMutableOptions}.
     *
     * @param keys the keys
     * @param values the values
     *
     * @return an instance of the options.
     */
    protected abstract T build(final String[] keys, final String[] values);

    public T build() {
      final String keys[] = new String[options.size()];
      final String values[] = new String[options.size()];

      int i = 0;
      for (final Map.Entry<K, MutableOptionValue<?>> option : options.entrySet()) {
        keys[i] = option.getKey().name();
        values[i] = option.getValue().asString();
        i++;
      }

      return build(keys, values);
    }

    protected U setDouble(
       final K key, final double value) {
      if (key.getValueType() != MutableOptionKey.ValueType.DOUBLE) {
        throw new IllegalArgumentException(
            key + " does not accept a double value");
      }
      options.put(key, MutableOptionValue.fromDouble(value));
      return self();
    }

    protected double getDouble(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asDouble();
    }

    protected U setLong(
        final K key, final long value) {
      if(key.getValueType() != MutableOptionKey.ValueType.LONG) {
        throw new IllegalArgumentException(
            key + " does not accept a long value");
      }
      options.put(key, MutableOptionValue.fromLong(value));
      return self();
    }

    protected long getLong(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asLong();
    }

    protected U setInt(
        final K key, final int value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT) {
        throw new IllegalArgumentException(
            key + " does not accept an integer value");
      }
      options.put(key, MutableOptionValue.fromInt(value));
      return self();
    }

    protected int getInt(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asInt();
    }

    protected U setBoolean(
        final K key, final boolean value) {
      if(key.getValueType() != MutableOptionKey.ValueType.BOOLEAN) {
        throw new IllegalArgumentException(
            key + " does not accept a boolean value");
      }
      options.put(key, MutableOptionValue.fromBoolean(value));
      return self();
    }

    protected boolean getBoolean(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asBoolean();
    }

    protected U setIntArray(
        final K key, final int[] value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT_ARRAY) {
        throw new IllegalArgumentException(
            key + " does not accept an int array value");
      }
      options.put(key, MutableOptionValue.fromIntArray(value));
      return self();
    }

    protected int[] getIntArray(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asIntArray();
    }

    protected <N extends Enum<N>> U setEnum(
        final K key, final N value) {
      if(key.getValueType() != MutableOptionKey.ValueType.ENUM) {
        throw new IllegalArgumentException(
            key + " does not accept a Enum value");
      }
      options.put(key, MutableOptionValue.fromEnum(value));
      return self();
    }

    protected <N extends Enum<N>> N getEnum(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }

      if(!(value instanceof MutableOptionValue.MutableOptionEnumValue)) {
        throw new NoSuchElementException(key.name() + " is not of Enum type");
      }

      return ((MutableOptionValue.MutableOptionEnumValue<N>)value).asObject();
    }

    public U fromString(
        final String keyStr, final String valueStr)
        throws IllegalArgumentException {
      Objects.requireNonNull(keyStr);
      Objects.requireNonNull(valueStr);

      final K key = allKeys().get(keyStr);
      switch(key.getValueType()) {
        case DOUBLE:
          return setDouble(key, Double.parseDouble(valueStr));

        case LONG:
          return setLong(key, Long.parseLong(valueStr));

        case INT:
          return setInt(key, Integer.parseInt(valueStr));

        case BOOLEAN:
          return setBoolean(key, Boolean.parseBoolean(valueStr));

        case INT_ARRAY:
          final String[] strInts = valueStr
              .trim().split(INT_ARRAY_INT_SEPARATOR);
          if(strInts == null || strInts.length == 0) {
            throw new IllegalArgumentException(
                "int array value is not correctly formatted");
          }

          final int value[] = new int[strInts.length];
          int i = 0;
          for(final String strInt : strInts) {
            value[i++] = Integer.parseInt(strInt);
          }
          return setIntArray(key, value);
      }

      throw new IllegalStateException(
          key + " has unknown value type: " + key.getValueType());
    }
  }
}

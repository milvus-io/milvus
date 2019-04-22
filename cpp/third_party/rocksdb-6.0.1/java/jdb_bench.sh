# shellcheck disable=SC2148
PLATFORM=64
if [ `getconf LONG_BIT` != "64" ]
then
  PLATFORM=32
fi

ROCKS_JAR=`find target -name rocksdbjni*.jar`

echo "Running benchmark in $PLATFORM-Bit mode."
# shellcheck disable=SC2068
java -server -d$PLATFORM -XX:NewSize=4m -XX:+AggressiveOpts -Djava.library.path=target -cp "${ROCKS_JAR}:benchmark/target/classes" org.rocksdb.benchmark.DbBenchmark $@

## Helper script for build_all.sh

FILE=$1

macro="$macro -DELPP_THREAD_SAFE"
macro="$macro -DELPP_STL_LOGGING"
macro="$macro -DELPP_FEATURE_CRASH_LOG"
macro="$macro -DELPP_FEATURE_ALL"

if [ "$2" = "" ];then
  COMPILER=g++
else
  COMPILER=$2
fi

CXX_STD='-std=c++0x -pthread'

if [ "$FILE" = "" ]; then
  echo "Please provide filename to compile"
  exit
fi

echo "Compiling... [$FILE]"

COMPILE_LINE="$COMPILER $FILE easylogging++.cc `pkg-config --libs --cflags gtkmm-2.4 sigc++-2.0` -o bin/$FILE.bin $macro $CXX_STD -Wall -Wextra -Wundef"
echo "    $COMPILE_LINE"

$($COMPILE_LINE)

echo "    DONE! [./bin/$FILE.bin]"
echo
echo

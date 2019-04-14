## Helper script for build_all.sh

macro="$macro -DELPP_THREAD_SAFE"
macro="$macro -DELPP_STL_LOGGING"
macro="$macro -DELPP_FEATURE_CRASH_LOG"

if [ "$1" = "" ];then
  COMPILER=g++
else
  COMPILER=$1
fi

CXX_STD='-std=c++0x -pthread'

COMPILE_LINE="$COMPILER *.cc `pkg-config --libs --cflags gtkmm-2.4 sigc++-2.0` -o hello.bin $macro $CXX_STD -Wall -Wextra"
echo "    $COMPILE_LINE"

$($COMPILE_LINE)

echo
echo

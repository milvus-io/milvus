## Helper script for build_all.sh

FILE=$1

if [ "$2" = "" ];then
  COMPILER=g++
else
  COMPILER=$2
fi

CXX_STD='-std=c++11'

if [ "$FILE" = "" ]; then
  echo "Please provide filename to compile"
  exit
fi

echo "Compiling... [$FILE]"

if [ ! -d "bin" ];then
  mkdir bin
fi;

COMPILE_LINE="$COMPILER $FILE easylogging++.cc -o bin/$FILE.bin $CXX_STD -pthread -lboost_system"

echo "    $COMPILE_LINE"

$($COMPILE_LINE)

echo "    DONE! [./bin/$FILE.bin]"
echo
echo

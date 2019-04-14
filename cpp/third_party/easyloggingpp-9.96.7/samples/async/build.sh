compiler=$1
macros=""
macros="$macros -DELPP_FEATURE_ALL"
if [ "$compiler" = "icpc" ];then
    macros="$macros -DELPP_NO_SLEEP_FOR"
fi
echo "$compiler prog.cpp easylogging++.cc -DELPP_EXPERIMENTAL_ASYNC $macros -std=c++11 -lpthread -o prog"
$compiler prog.cpp easylogging++.cc -DELPP_EXPERIMENTAL_ASYNC $macros -std=c++11 -lpthread -o prog

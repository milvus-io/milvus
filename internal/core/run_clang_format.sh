if [ -z $1 ]; then
    echo "usage: $0 <path_to_core>"
    exit -1
else
    echo start formating
fi
CorePath=$1

formatThis() {
    find "$1" | grep -E "(*\.cpp|*\.h|*\.cc)$" | grep -v "gen_tools/templates" | grep -v "/thirdparty" | grep -v "\.pb\." | xargs clang-format-10 -i
}

formatThis "${CorePath}/src"
formatThis "${CorePath}/unittest"
formatThis "${CorePath}/unittest/bench"

${CorePath}/build-support/add_cpp_license.sh ${CorePath}/build-support/cpp_license.txt ${CorePath}
${CorePath}/build-support/add_cmake_license.sh ${CorePath}/build-support/cmake_license.txt ${CorePath}

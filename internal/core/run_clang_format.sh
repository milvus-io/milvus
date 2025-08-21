if [ -z $1 ]; then
    echo "usage: $0 <path_to_core>"
    exit -1
else
    echo start formating
fi
CorePath=$1

formatThis() {
    find "$1" | grep -E "(*\.cpp|*\.h|*\.cc)$" | grep -v "gen_tools/templates" | grep -v "\.pb\." | grep -v "tantivy-binding.h" | xargs clang-format -i
}

formatThis "${CorePath}/src"
formatThis "${CorePath}/unittest"
formatThis "${CorePath}/unittest/bench"
formatThis "${CorePath}/thirdparty/tantivy"

${CorePath}/build-support/add_cpp_license.sh ${CorePath}/build-support/cpp_license.txt ${CorePath}
${CorePath}/build-support/add_cmake_license.sh ${CorePath}/build-support/cmake_license.txt ${CorePath}

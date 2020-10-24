if [ -z $1 ]; then
    echo "usage: $0 <path_to_core>"
    exit -1
else
    echo start formating
fi
CorePath=$1

formatThis() {
    find "$1" | grep -E "(*\.cpp|*\.h|*\.cc)$" | grep -v "/thirdparty" | grep -v "\.pb\." | xargs clang-format -i
}

formatThis "${CorePath}/src"
formatThis "${CorePath}/unittest"


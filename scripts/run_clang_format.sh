if [ -z $1 ]; then
    echo "usage: $0 <path_to_core>"
    exit -1
fi
CorePath=$1

formatThis() {
    find "$1" | grep -E "(*\.cpp|*\.h|*\.cc)$" | grep -v "gen_tools/templates" | grep -v "/thirdparty" | grep -v "\.pb\." | xargs clang-format-10 -i
}

formatThis "${CorePath}/src"
formatThis "${CorePath}/unittest"

if test -z "$(git status | grep -E "*\.c|*\.h")"; then
  exit 0
else
  echo "Please format your code by clang-format!"
  exit 1
fi

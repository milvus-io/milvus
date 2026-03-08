LICENSE=$1
FOLDER=$2

if [ -z ${FOLDER} ] || [ -z ${LICENSE} ]; then
    echo "usage $0 <path/to/license> <path/to/code_folder>"
    exit
fi

cat ${LICENSE} > /dev/null || exit -1

FILES=`find ${FOLDER} \
| grep "CMakeLists.txt$" \
| grep -v thirdparty \
| grep -v cmake_build \
| grep -v cmake-build \
| grep -v "build/" \
| grep -v output \
| grep -v "\.pb\."`
# echo formating ${FILES} ...
skip_count=0
for f in ${FILES}; do
  if (grep "Apache License" $f > /dev/null);then 
    # echo "No need to copy the License Header to $f"
    skip_count=$((skip_count+1))
  else
    cat ${LICENSE} $f > $f.new
    mv $f.new $f
    echo "License Header copied to $f"
  fi 
done   

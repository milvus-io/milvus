
# Builds all files into bin/

[ -d "bin" ] || mkdir "bin"
rm -rf bin/*
EXCLUDE_LIST="logrotate.cpp"
find . -maxdepth 1 -type f -name '*.cpp' -not -name $EXCLUDE_LIST -exec sh compile.sh {} $1 \;
echo "Completed!"

files=$(ls -l bin/)
if [ "$files" = "total 0" ];then
  exit 1
else
  exit 0
fi

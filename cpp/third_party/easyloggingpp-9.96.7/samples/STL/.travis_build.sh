sh ./build_all.sh clang++
if [ "$?" = "0" ];then
  echo "Built successfully"
  sh ./run_all.sh
  echo "Successfully ran all samples"
else
  echo "Build failed! (code: $?)"
  exit $?
fi

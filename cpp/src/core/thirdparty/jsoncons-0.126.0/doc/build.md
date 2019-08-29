Start Visual Studio Command prompt for x64

mkdir build64 & pushd build64
cmake -G "Visual Studio 14 2015 Win64" ..
popd
cmake --build build64 --config Debug

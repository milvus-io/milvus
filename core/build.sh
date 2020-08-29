if [[ -d "./build" ]]
then
    rm -rf build
fi

mkdir build && cd build
cmake ..
make -j8 && make install
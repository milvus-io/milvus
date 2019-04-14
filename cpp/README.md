# Compilation
##### Step 1, build the third_party library:
./third_party/build.sh

##### step2, build vecwise engine:
mkdir build && cd build && cmake .. && make -j

##### step3, get the library and server:
build/src/vecwise_engine_server is the server
build/src/libvecwise_engine.a is the static library
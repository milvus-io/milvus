
# Builds all tests into bin/ and runs

[ -d "bin" ] || mkdir "bin"

cd bin
echo "Building..."
g++ ../*.cc -std=c++11 -pthread -lgtest -DELPP_NO_DEFAULT_LOG_FILE -DELPP_FEATURE_ALL -DELPP_LOGGING_FLAGS_FROM_ARG -Wall -Wextra -pedantic -pedantic-errors -Werror -Wfatal-errors -Wundef -o easyloggingpp-test && echo "Running..." && ./easyloggingpp-test -v
cd ..
echo "Completed!"

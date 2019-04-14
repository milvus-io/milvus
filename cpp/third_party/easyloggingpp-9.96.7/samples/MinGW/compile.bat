echo Assuming C:\MinGW for MinGW

set path=%path%;C:\MinGW\bin\

"C:\MinGW\bin\g++.exe" -o prog.exe prog.cpp easylogging++.cc -std=c++11 -DELPP_FEATURE_ALL

echo "Building fastCover dictionary with in=../../lib/common f=20 out=dict1"
./main in=../../../lib/common f=20 out=dict1
zstd -be3 -D dict1 -r ../../../lib/common -q
echo "Building fastCover dictionary with in=../../lib/common k=500 d=6 f=24 out=dict2 dictID=100 maxdict=140000"
./main in=../../../lib/common k=500 d=6 f=24 out=dict2 dictID=100 maxdict=140000
zstd -be3 -D dict2 -r ../../../lib/common -q
echo "Building fastCover dictionary with 2 sample sources"
./main in=../../../lib/common in=../../../lib/compress out=dict3
zstd -be3 -D dict3 -r ../../../lib/common -q
echo "Removing dict1 dict2 dict3"
rm -f dict1 dict2 dict3

echo "Testing with invalid parameters, should fail"
! ./main in=../../../lib/common r=10
! ./main in=../../../lib/common d=10

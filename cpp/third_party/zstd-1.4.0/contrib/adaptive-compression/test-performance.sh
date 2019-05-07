echo "testing time -- no limits set"
./datagen -s1 -g1GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
rm tmp*

./datagen -s2 -g2GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
rm tmp*

./datagen -s3 -g4GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
rm tmp*

echo -e "\ntesting compression ratio -- no limits set"
./datagen -s4 -g1GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
ls -l tmp1.zst tmp2.zst
rm tmp*

./datagen -s5 -g2GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
ls -l tmp1.zst tmp2.zst
rm tmp*

./datagen -s6 -g4GB > tmp
time ./adapt -otmp1.zst tmp
time zstd -1 -o tmp2.zst tmp
ls -l tmp1.zst tmp2.zst
rm tmp*

echo e "\ntesting performance at various compression levels -- no limits set"
./datagen -s7 -g1GB > tmp
echo "adapt"
time ./adapt -i5 -f tmp -otmp1.zst
echo "zstdcli"
time zstd -5 tmp -o tmp2.zst
ls -l tmp1.zst tmp2.zst
rm tmp*

./datagen -s8 -g1GB > tmp
echo "adapt"
time ./adapt -i10 -f tmp -otmp1.zst
echo "zstdcli"
time zstd -10 tmp -o tmp2.zst
ls -l tmp1.zst tmp2.zst
rm tmp*

./datagen -s9 -g1GB > tmp
echo "adapt"
time ./adapt -i15 -f tmp -otmp1.zst
echo "zstdcli"
time zstd -15 tmp -o tmp2.zst
ls -l tmp1.zst tmp2.zst
rm tmp*

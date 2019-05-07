echo "correctness tests -- general"
./datagen -s1 -g1GB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s2 -g500MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s3 -g250MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s4 -g125MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s5 -g50MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s6 -g25MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s7 -g10MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s8 -g5MB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s9 -g500KB > tmp
./adapt -otmp.zst tmp
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- streaming"
./datagen -s10 -g1GB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s11 -g100MB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s12 -g10MB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s13 -g1MB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s14 -g100KB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s15 -g10KB > tmp
cat tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- read limit"
./datagen -s16 -g1GB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s17 -g100MB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s18 -g10MB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s19 -g1MB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s20 -g100KB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s21 -g10KB > tmp
pv -L 50m -q tmp | ./adapt > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- write limit"
./datagen -s22 -g1GB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s23 -g100MB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s24 -g10MB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s25 -g1MB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s26 -g100KB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s27 -g10KB > tmp
pv -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- read and write limits"
./datagen -s28 -g1GB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s29 -g100MB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s30 -g10MB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s31 -g1MB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s32 -g100KB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s33 -g10KB > tmp
pv -L 50m -q tmp | ./adapt | pv -L 5m -q > tmp.zst
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- forced compression level"
./datagen -s34 -g1GB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s35 -g100MB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s36 -g10MB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s37 -g1MB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s38 -g100KB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

./datagen -s39 -g10KB > tmp
./adapt tmp -otmp.zst -i11 -f
zstd -d tmp.zst -o tmp2
diff -s -q tmp tmp2
rm tmp*

echo -e "\ncorrectness tests -- window size test"
./datagen -s39 -g1GB | pv -L 25m -q | ./adapt -i1 | pv -q > tmp.zst
zstd -d tmp.zst
rm tmp*

echo -e "\ncorrectness tests -- testing bounds"
./datagen -s40 -g1GB | pv -L 25m -q | ./adapt -i1 -u4 | pv -q > tmp.zst
rm tmp*

./datagen -s41 -g1GB | ./adapt -i14 -l4 > tmp.zst
rm tmp*
make clean

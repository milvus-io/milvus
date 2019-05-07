Programs and scripts for automated testing of LZ4
=======================================================

This directory contains the following programs and scripts:
- `datagen` : Synthetic and parametrable data generator, for tests
- `frametest` : Test tool that checks lz4frame integrity on target platform
- `fullbench`  : Precisely measure speed for each lz4 inner functions
- `fuzzer`  : Test tool, to check lz4 integrity on target platform
- `test-lz4-speed.py` : script for testing lz4 speed difference between commits
- `test-lz4-versions.py` : compatibility test between lz4 versions stored on Github


#### `test-lz4-versions.py` - script for testing lz4 interoperability between versions

This script creates `versionsTest` directory to which lz4 repository is cloned.
Then all taged (released) versions of lz4 are compiled.
In the following step interoperability between lz4 versions is checked.


#### `test-lz4-speed.py` - script for testing lz4 speed difference between commits

This script creates `speedTest` directory to which lz4 repository is cloned.
Then it compiles all branches of lz4 and performs a speed benchmark for a given list of files (the `testFileNames` parameter).
After `sleepTime` (an optional parameter, default 300 seconds) seconds the script checks repository for new commits.
If a new commit is found it is compiled and a speed benchmark for this commit is performed.
The results of the speed benchmark are compared to the previous results.
If compression or decompression speed for one of lz4 levels is lower than `lowerLimit` (an optional parameter, default 0.98) the speed benchmark is restarted.
If second results are also lower than `lowerLimit` the warning e-mail is send to recipients from the list (the `emails` parameter).

Additional remarks:
- To be sure that speed results are accurate the script should be run on a "stable" target system with no other jobs running in parallel
- Using the script with virtual machines can lead to large variations of speed results
- The speed benchmark is not performed until computers' load average is lower than `maxLoadAvg` (an optional parameter, default 0.75)
- The script sends e-mails using `mutt`; if `mutt` is not available it sends e-mails without attachments using `mail`; if both are not available it only prints a warning


The example usage with two test files, one e-mail address, and with an additional message:
```
./test-lz4-speed.py "silesia.tar calgary.tar" "email@gmail.com" --message "tested on my laptop" --sleepTime 60
``` 

To run the script in background please use:
```
nohup ./test-lz4-speed.py testFileNames emails &
```

The full list of parameters:
```
positional arguments:
  testFileNames         file names list for speed benchmark
  emails                list of e-mail addresses to send warnings

optional arguments:
  -h, --help            show this help message and exit
  --message MESSAGE     attach an additional message to e-mail
  --lowerLimit LOWERLIMIT
                        send email if speed is lower than given limit
  --maxLoadAvg MAXLOADAVG
                        maximum load average to start testing
  --lastCLevel LASTCLEVEL
                        last compression level for testing
  --sleepTime SLEEPTIME
                        frequency of repository checking in seconds
```


#### License

All files in this directory are licensed under GPL-v2.
See [COPYING](COPYING) for details.
The text of the license is also included at the top of each source file.

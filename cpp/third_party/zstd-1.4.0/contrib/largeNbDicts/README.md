largeNbDicts
=====================

`largeNbDicts` is a benchmark test tool
dedicated to the specific scenario of
dictionary decompression using a very large number of dictionaries.
When dictionaries are constantly changing, they are always "cold",
suffering from increased latency due to cache misses.

The tool is created in a bid to investigate performance for this scenario,
and experiment mitigation techniques.

Command line :
```
largeNbDicts [Options] filename(s)

Options :
-r           : recursively load all files in subdirectories (default: off)
-B#          : split input into blocks of size # (default: no split)
-#           : use compression level # (default: 3)
-D #         : use # as a dictionary (default: create one)
-i#          : nb benchmark rounds (default: 6)
--nbDicts=#  : set nb of dictionaries to # (default: one per block)
-h           : help (this text)
```

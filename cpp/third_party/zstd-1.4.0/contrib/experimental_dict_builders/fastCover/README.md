FastCover Dictionary Builder

### Permitted Arguments:
Input File/Directory (in=fileName): required; file/directory used to build dictionary; if directory, will operate recursively for files inside directory; can include multiple files/directories, each following "in="
Output Dictionary (out=dictName): if not provided, default to fastCoverDict
Dictionary ID (dictID=#): nonnegative number; if not provided, default to 0
Maximum Dictionary Size (maxdict=#): positive number; in bytes, if not provided, default to 110KB
Size of Selected Segment (k=#): positive number; in bytes; if not provided, default to 200
Size of Dmer (d=#): either 6 or 8; if not provided, default to 8
Number of steps (steps=#): positive number, if not provided, default to 32
Percentage of samples used for training(split=#): positive number; if not provided, default to 100


###Running Test:
make test


###Usage:
To build a FASTCOVER dictionary with the provided arguments: make ARG= followed by arguments
If k or d is not provided, the optimize version of FASTCOVER is run.

### Examples:
make ARG="in=../../../lib/dictBuilder out=dict100 dictID=520"
make ARG="in=../../../lib/dictBuilder in=../../../lib/compress"

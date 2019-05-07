Random Dictionary Builder

### Permitted Arguments:
Input File/Directory (in=fileName): required; file/directory used to build dictionary; if directory, will operate recursively for files inside directory; can include multiple files/directories, each following "in="
Output Dictionary (out=dictName): if not provided, default to defaultDict
Dictionary ID (dictID=#): nonnegative number; if not provided, default to 0
Maximum Dictionary Size (maxdict=#): positive number; in bytes, if not provided, default to 110KB
Size of Randomly Selected Segment (k=#): positive number; in bytes; if not provided, default to 200

###Running Test:
make test


###Usage:
To build a random dictionary with the provided arguments: make ARG= followed by arguments


### Examples:
make ARG="in=../../../lib/dictBuilder out=dict100 dictID=520"
make ARG="in=../../../lib/dictBuilder in=../../../lib/compress"

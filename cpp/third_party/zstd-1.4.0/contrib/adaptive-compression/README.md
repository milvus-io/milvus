### Summary

`adapt` is a new compression tool targeted at optimizing performance across network connections and pipelines. The tool is aimed at sensing network speeds and adapting compression level based on network or pipe speeds.
In situations where the compression level does not appropriately match the network/pipe speed, compression may be bottlenecking the entire pipeline or the files may not be compressed as much as they potentially could be, therefore losing efficiency. It also becomes quite impractical to manually measure and set an optimalcompression level (which could potentially change over time). 

### Using `adapt`

In order to build and use the tool, you can simply run `make adapt` in the `adaptive-compression` directory under `contrib`. This will generate an executable available for use. Another possible method of installation is running `make install`, which will create and install the binary as the command `zstd-adaptive`.

Similar to many other compression utilities, `zstd-adaptive` can be invoked by using the following format:

`zstd-adaptive [options] [file(s)]`

Supported options for the above format are described below. 

`zstd-adaptive` also supports reading from `stdin` and writing to `stdout`, which is potentially more useful. By default, if no files are given, `zstd-adaptive` reads from and writes to standard I/O. Therefore, you can simply insert it within a pipeline like so:

`cat FILE | zstd-adaptive | ssh "cat - > tmp.zst"`

If a file is provided, it is also possible to force writing to stdout using the `-c` flag like so:

`zstd-adaptive -c FILE | ssh "cat - > tmp.zst"`

Several options described below can be used to control the behavior of `zstd-adaptive`. More specifically, using the `-l#` and `-u#` flags will will set upper and lower bounds so that the compression level will always be within that range. The `-i#` flag can also be used to change the initial compression level. If an initial compression level is not provided, the initial compression level will be chosen such that it is within the appropriate range (it becomes equal to the lower bound). 

### Options
`-oFILE` : write output to `FILE`

`-i#`    : provide initial compression level (must within the appropriate bounds)

`-h`     : display help/information

`-f`     : force the compression level to stay constant

`-c`     : force write to `stdout`

`-p`     : hide progress bar

`-q`     : quiet mode -- do not show progress bar or other information

`-l#`    : set a lower bound on the compression level (default is 1)

`-u#`    : set an upper bound on the compression level (default is 22)
### Benchmarking / Test results
#### Artificial Tests
These artificial tests were run by using the `pv` command line utility in order to limit pipe speeds (25 MB/s read and 5 MB/s write limits were chosen to mimic severe throughput constraints). A 40 GB backup file was sent through a pipeline, compressed, and written out to a file. Compression time, size, and ratio were computed. Data for `zstd -15` was excluded from these tests because the test runs quite long.

<table>
<tr><th> 25 MB/s read limit </th></tr>
<tr><td>

| Compressor Name | Ratio | Compressed Size | Compression Time |
|:----------------|------:|----------------:|-----------------:| 
| zstd -3         | 2.108 |       20.718 GB |      29m 48.530s |
| zstd-adaptive   | 2.230 |       19.581 GB |      29m 48.798s |

</td><tr>
</table>

<table>
<tr><th> 5 MB/s write limit </th></tr>
<tr><td>

| Compressor Name | Ratio | Compressed Size | Compression Time |
|:----------------|------:|----------------:|-----------------:| 
| zstd -3         | 2.108 |       20.718 GB |   1h 10m 43.076s |
| zstd-adaptive   | 2.249 |       19.412 GB |   1h 06m 15.577s |

</td></tr>
</table>

The commands used for this test generally followed the form:

`cat FILE | pv -L 25m -q | COMPRESSION | pv -q > tmp.zst # impose 25 MB/s read limit`

`cat FILE | pv -q | COMPRESSION | pv -L 5m -q > tmp.zst  # impose 5 MB/s write limit`

#### SSH Tests

The following tests were performed by piping a relatively large backup file (approximately 80 GB) through compression and over SSH to be stored on a server. The test data includes statistics for time and compressed size  on `zstd` at several compression levels, as well as `zstd-adaptive`. The data highlights the potential advantages that `zstd-adaptive` has over using a low static compression level and the negative imapcts that using an excessively high static compression level can have on
pipe throughput.

| Compressor Name | Ratio | Compressed Size | Compression Time |
|:----------------|------:|----------------:|-----------------:|
| zstd -3         | 2.212 |       32.426 GB |   1h 17m 59.756s |
| zstd -15        | 2.374 |       30.213 GB |   2h 56m 59.441s |
| zstd-adaptive   | 2.315 |       30.993 GB |   1h 18m 52.860s |

The commands used for this test generally followed the form: 

`cat FILE | COMPRESSION | ssh dev "cat - > tmp.zst"`

# ################################################################
# Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# ################################################################

# This Makefile presumes libzstd is installed, using `sudo make install`

CPPFLAGS += -I../lib
LIB = ../lib/libzstd.a

.PHONY: default all clean test

default: all

all: simple_compression simple_decompression \
	multiple_simple_compression\
	dictionary_compression dictionary_decompression \
	streaming_compression streaming_decompression \
	multiple_streaming_compression streaming_memory_usage

$(LIB) :
	$(MAKE) -C ../lib libzstd.a

simple_compression : simple_compression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

simple_decompression : simple_decompression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

multiple_simple_compression : multiple_simple_compression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

dictionary_compression : dictionary_compression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

dictionary_decompression : dictionary_decompression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

streaming_compression : streaming_compression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

multiple_streaming_compression : multiple_streaming_compression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

streaming_decompression : streaming_decompression.c common.h $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

streaming_memory_usage : streaming_memory_usage.c $(LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

clean:
	@rm -f core *.o tmp* result* *.zst \
        simple_compression simple_decompression \
        multiple_simple_compression \
        dictionary_compression dictionary_decompression \
        streaming_compression streaming_decompression \
        multiple_streaming_compression streaming_memory_usage
	@echo Cleaning completed

test: all
	cp README.md tmp
	cp Makefile tmp2
	@echo -- Simple compression tests
	./simple_compression tmp
	./simple_decompression tmp.zst
	./multiple_simple_compression *.c
	./streaming_decompression tmp.zst > /dev/null
	@echo -- Streaming memory usage
	./streaming_memory_usage
	@echo -- Streaming compression tests
	./streaming_compression tmp
	./streaming_decompression tmp.zst > /dev/null
	@echo -- Edge cases detection
	! ./streaming_decompression tmp    # invalid input, must fail
	! ./simple_decompression tmp       # invalid input, must fail
	! ./simple_decompression tmp.zst   # unknown input size, must fail
	touch tmpNull                      # create 0-size file
	./simple_compression tmpNull
	./simple_decompression tmpNull.zst # 0-size frame : must work
	@echo -- Multiple streaming tests
	./multiple_streaming_compression *.c
	@echo -- Dictionary compression tests
	./dictionary_compression tmp2 tmp README.md
	./dictionary_decompression tmp2.zst tmp.zst README.md
	$(RM) tmp* *.zst
	@echo tests completed

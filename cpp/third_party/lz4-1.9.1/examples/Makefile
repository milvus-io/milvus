# ##########################################################################
# LZ4 examples - Makefile
# Copyright (C) Yann Collet 2011-2014
#
# GPL v2 License
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# You can contact the author at :
#  - LZ4 source repository : https://github.com/Cyan4973/lz4
#  - LZ4 forum froup : https://groups.google.com/forum/#!forum/lz4c
# ##########################################################################
# This makefile compile and test
# example programs, using (mostly) LZ4 streaming library,
# kindly provided by Takayuki Matsuoka
# ##########################################################################

CPPFLAGS += -I../lib
CFLAGS   ?= -O3
CFLAGS   += -std=gnu99 -Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes
FLAGS    := $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $(MOREFLAGS)

TESTFILE  = Makefile
LZ4DIR   := ../lib
LZ4       = ../programs/lz4

include ../Makefile.inc

default: all

all: printVersion doubleBuffer dictionaryRandomAccess ringBuffer ringBufferHC \
     lineCompress frameCompress simpleBuffer

$(LZ4DIR)/liblz4.a: $(LZ4DIR)/lz4.c $(LZ4DIR)/lz4hc.c $(LZ4DIR)/lz4frame.c $(LZ4DIR)/lz4.h $(LZ4DIR)/lz4hc.h $(LZ4DIR)/lz4frame.h $(LZ4DIR)/lz4frame_static.h
	$(MAKE) -C $(LZ4DIR) liblz4.a

printVersion: printVersion.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

doubleBuffer: blockStreaming_doubleBuffer.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

dictionaryRandomAccess: dictionaryRandomAccess.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

ringBuffer  : blockStreaming_ringBuffer.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

ringBufferHC: HCStreaming_ringBuffer.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

lineCompress: blockStreaming_lineByLine.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

frameCompress: frameCompress.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

compressFunctions: compress_functions.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT) -lrt

simpleBuffer: simple_buffer.c $(LZ4DIR)/liblz4.a
	$(CC) $(FLAGS) $^ -o $@$(EXT)

$(LZ4) :
	$(MAKE) -C ../programs lz4

test : all $(LZ4)
	@echo "\n=== Print Version ==="
	./printVersion$(EXT)
	@echo "\n=== Simple compression example ==="
	./simpleBuffer$(EXT)
	@echo "\n=== Double-buffer ==="
	./doubleBuffer$(EXT) $(TESTFILE)
	@echo "\n=== Ring Buffer ==="
	./ringBuffer$(EXT)   $(TESTFILE)
	@echo "\n=== Ring Buffer + LZ4 HC ==="
	./ringBufferHC$(EXT) $(TESTFILE)
	@echo "\n=== Compress line by line ==="
	./lineCompress$(EXT) $(TESTFILE)
	@echo "\n=== Dictionary Random Access ==="
	./dictionaryRandomAccess$(EXT) $(TESTFILE) $(TESTFILE) 1100 1400
	@echo "\n=== Frame compression ==="
	./frameCompress$(EXT) $(TESTFILE)
	$(LZ4) -vt $(TESTFILE).lz4

clean:
	@rm -f core *.o *.dec *-0 *-9 *-8192 *.lz4s *.lz4 \
	 printVersion$(EXT) doubleBuffer$(EXT) dictionaryRandomAccess$(EXT) \
	 ringBuffer$(EXT) ringBufferHC$(EXT) lineCompress$(EXT) frameCompress$(EXT) \
	 compressFunctions$(EXT) simpleBuffer$(EXT)
	@echo Cleaning completed

ARG :=

CC ?= gcc
CFLAGS ?= -O3 -g
INCLUDES := -I ../../../programs -I ../randomDictBuilder -I ../../../lib/common -I ../../../lib -I ../../../lib/dictBuilder

IO_FILE := ../randomDictBuilder/io.c

TEST_INPUT := ../../../lib
TEST_OUTPUT := fastCoverDict

all: main run clean

.PHONY: test
test: main testrun testshell clean

.PHONY: run
run:
	echo "Building a fastCover dictionary with given arguments"
	./main $(ARG)

main: main.o io.o fastCover.o libzstd.a
	$(CC) $(CFLAGS) main.o io.o fastCover.o libzstd.a -o main

main.o: main.c
	$(CC) $(CFLAGS) $(INCLUDES) -c main.c

fastCover.o: fastCover.c
	$(CC) $(CFLAGS) $(INCLUDES) -c fastCover.c

io.o: $(IO_FILE)
	$(CC) $(CFLAGS) $(INCLUDES) -c $(IO_FILE)

libzstd.a:
	$(MAKE) MOREFLAGS=-g -C ../../../lib libzstd.a
	mv ../../../lib/libzstd.a .

.PHONY: testrun
testrun: main
	echo "Run with $(TEST_INPUT) and $(TEST_OUTPUT) "
	./main in=$(TEST_INPUT) out=$(TEST_OUTPUT)
	zstd -be3 -D $(TEST_OUTPUT) -r $(TEST_INPUT) -q
	rm -f $(TEST_OUTPUT)

.PHONY: testshell
testshell: test.sh
	sh test.sh
	echo "Finish running test.sh"

.PHONY: clean
clean:
	rm -f *.o main libzstd.a
	$(MAKE) -C ../../../lib clean
	echo "Cleaning is completed"

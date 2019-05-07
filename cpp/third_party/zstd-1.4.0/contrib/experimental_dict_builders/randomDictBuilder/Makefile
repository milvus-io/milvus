ARG :=

CC ?= gcc
CFLAGS ?= -O3
INCLUDES := -I ../../../programs -I ../../../lib/common -I ../../../lib -I ../../../lib/dictBuilder

TEST_INPUT := ../../../lib
TEST_OUTPUT := randomDict

all: main run clean

.PHONY: test
test: main testrun testshell clean

.PHONY: run
run:
	echo "Building a random dictionary with given arguments"
	./main $(ARG)

main: main.o io.o random.o libzstd.a
	$(CC) $(CFLAGS) main.o io.o random.o libzstd.a -o main

main.o: main.c
	$(CC) $(CFLAGS) $(INCLUDES) -c main.c

random.o: random.c
	$(CC) $(CFLAGS) $(INCLUDES) -c random.c

io.o: io.c
	$(CC) $(CFLAGS) $(INCLUDES) -c io.c

libzstd.a:
	$(MAKE) -C ../../../lib libzstd.a
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

CC = g++
CFLAGS = -Wall -std=c++11
PROG = cube

SRCS = main.cpp imageloader.cpp

ifeq ($(shell uname),Darwin)
	LIBS = -framework OpenGL -framework GLUT
else
	LIBS = -lglut -lGLU -lGL
endif

all: $(PROG)

$(PROG):	$(SRCS)
	$(CC) $(CFLAGS) -o bin/$(PROG) $(SRCS) $(LIBS)

clean:
	rm -f $(PROG)

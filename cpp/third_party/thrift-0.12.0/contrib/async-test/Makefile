THRIFT = thrift
CXXFLAGS = `pkg-config --cflags thrift thrift-nb` -levent
LDLIBS = `pkg-config --libs thrift thrift-nb` -levent
CXXFLAGS += -g -O0

GENNAMES = Aggr aggr_types
GENHEADERS = $(addsuffix .h, $(GENNAMES))
GENSRCS = $(addsuffix .cpp, $(GENNAMES))
GENOBJS = $(addsuffix .o, $(GENNAMES))

PYLIBS = aggr/__init__.py

PROGS =  test-server

all: $(PYLIBS) $(PROGS)

test-server: test-server.o $(GENOBJS)

test-server.o: $(GENSRCS)

aggr/__init__.py: aggr.thrift
	$(RM) $(dir $@)
	$(THRIFT) --gen py:newstyle $<
	mv gen-py/$(dir $@) .

$(GENSRCS): aggr.thrift
	$(THRIFT) --gen cpp:cob_style $<
	mv $(addprefix gen-cpp/, $(GENSRCS) $(GENHEADERS)) .

clean:
	$(RM) -r *.o $(PROGS) $(GENSRCS) $(GENHEADERS) gen-* aggr

.PHONY: clean

# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

.SUFFIXES: .cpp .o

# C API

include ../makefile.inc
DEBUGFLAG=-DNDEBUG # no debugging

LIBNAME=libfaiss
CLIBNAME=libfaiss_c
LIBCOBJ=error_impl.o Index_c.o IndexFlat_c.o Clustering_c.o AutoTune_c.o \
	impl/AuxIndexStructures_c.o IndexIVF_c.o IndexIVFFlat_c.o IndexLSH_c.o \
	index_io_c.o MetaIndexes_c.o IndexShards_c.o index_factory_c.o \
	clone_index_c.o IndexPreTransform_c.o
CFLAGS=-fPIC -m64 -Wno-sign-compare -g -O3 -Wall -Wextra

# Build static and shared object files by default
all: $(CLIBNAME).a $(CLIBNAME).$(SHAREDEXT)

# Build static object file containing the wrapper implementation only.
# Consumers are required to link with libfaiss.a and libstdc++.
$(CLIBNAME).a: $(LIBCOBJ)
	ar r $@ $^

# Build dynamic library (independent object)
$(CLIBNAME).$(SHAREDEXT): $(LIBCOBJ) ../$(LIBNAME).a
	$(CXX) $(LDFLAGS) $(SHAREDFLAGS) -o $@ \
	-Wl,--whole-archive $^ -Wl,--no-whole-archive $(LIBS) -static-libstdc++

bin/example_c: example_c.c $(CLIBNAME).$(SHAREDEXT)
	$(CC) $(CFLAGS) -std=c99 -I. -I.. -L. -o $@ example_c.c \
	$(LDFLAGS) -lm -lfaiss_c

clean:
	rm -f $(CLIBNAME).a $(CLIBNAME).$(SHAREDEXT)* *.o bin/example_c

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CPUFLAGS) -c $< -o $@

# Dependencies

error_impl.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
error_impl.o: error_impl.cpp error_c.h error_impl.h macros_impl.h

index_io_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
index_io_c.o: index_io_c.cpp error_impl.cpp ../index_io.h macros_impl.h

index_factory_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
index_factory_c.o: index_factory_c.cpp error_impl.cpp ../index_io.h macros_impl.h

clone_index_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
clone_index_c.o: index_factory_c.cpp error_impl.cpp ../index_io.h macros_impl.h

Index_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
Index_c.o: Index_c.cpp Index_c.h ../Index.h macros_impl.h

IndexFlat_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexFlat_c.o: IndexFlat_c.cpp IndexFlat_c.h ../IndexFlat.h macros_impl.h

IndexIVF_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexIVF_c.o: IndexIVF_c.cpp IndexIVF_c.h ../IndexIVF.h macros_impl.h

IndexIVFFlat_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexIVFFlat_c.o: IndexIVFFlat_c.cpp IndexIVFFlat_c.h ../IndexIVFFlat.h macros_impl.h

IndexLSH_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexLSH_c.o: IndexLSH_c.cpp IndexLSH_c.h ../IndexLSH.h macros_impl.h

IndexShards_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexShards_c.o: IndexShards_c.cpp IndexShards_c.h ../Index.h ../IndexShards.h macros_impl.h

Clustering_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
Clustering_c.o: Clustering_c.cpp Clustering_c.h ../Clustering.h macros_impl.h

AutoTune_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
AutoTune_c.o: AutoTune_c.cpp AutoTune_c.h ../AutoTune.h macros_impl.h

impl/AuxIndexStructures_c.o: CXXFLAGS += -I..  -I ../impl $(DEBUGFLAG)
impl/AuxIndexStructures_c.o: impl/AuxIndexStructures_c.cpp impl/AuxIndexStructures_c.h ../impl/AuxIndexStructures.h macros_impl.h

MetaIndexes_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
MetaIndexes_c.o: MetaIndexes_c.cpp MetaIndexes_c.h ../MetaIndexes.h macros_impl.h

IndexPreTransform_c.o: CXXFLAGS += -I.. -I ../impl $(DEBUGFLAG)
IndexPreTransform_c.o: IndexPreTransform_c.cpp IndexPreTransform_c.h ../IndexPreTransform.h macros_impl.h

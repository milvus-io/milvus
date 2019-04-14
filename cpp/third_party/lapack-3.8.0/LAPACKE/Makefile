##############################################################################
# Copyright (c) 2010, Intel Corp.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of Intel Corporation nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
##############################################################################
# Contents: Native C interface to LAPACK
# Author: Intel Corporation
# September, 2010
##############################################################################
# Top-level makefile for LAPACKE.
#
# To generate lapacke library type 'make lapacke'
# To make both just type 'make'
#
# To remove lapacke object files type 'make cleanobj'
# To clean all above type 'make clean'
# To clean everything including lapacke library type
# 'make cleanall'
#
include ../make.inc

all: lapacke

lapacke: include/lapacke_mangling.h
	$(MAKE) -C src
	$(MAKE) -C utils

include/lapacke_mangling.h: include/lapacke_mangling_with_flags.h.in
	cp $< $@

lapacke_example: lapacke
	$(MAKE) -C example

clean: cleanlib
	$(MAKE) -C src clean
	$(MAKE) -C utils clean
	$(MAKE) -C example clean
cleanobj:
	$(MAKE) -C src cleanobj
	$(MAKE) -C utils cleanobj
	$(MAKE) -C example cleanobj
cleanlib:
	rm -f ../$(LAPACKELIB)
cleanexe:
	$(MAKE) -C example cleanexe

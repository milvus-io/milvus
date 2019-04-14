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
# makefile for LAPACKE, used to build lapacke binary.
#
include ../../make.inc

OBJ = lapacke_cgb_nancheck.o \
      lapacke_cgb_trans.o \
      lapacke_cge_nancheck.o \
      lapacke_cge_trans.o \
      lapacke_cgg_nancheck.o \
      lapacke_cgg_trans.o \
      lapacke_cgt_nancheck.o \
      lapacke_chb_nancheck.o \
      lapacke_chb_trans.o \
      lapacke_che_nancheck.o \
      lapacke_che_trans.o \
      lapacke_chp_nancheck.o \
      lapacke_chp_trans.o \
      lapacke_chs_nancheck.o \
      lapacke_chs_trans.o \
      lapacke_c_nancheck.o \
      lapacke_cpb_nancheck.o \
      lapacke_cpb_trans.o \
      lapacke_cpf_nancheck.o \
      lapacke_cpf_trans.o \
      lapacke_cpo_nancheck.o \
      lapacke_cpo_trans.o \
      lapacke_cpp_nancheck.o \
      lapacke_cpp_trans.o \
      lapacke_cpt_nancheck.o \
      lapacke_csp_nancheck.o \
      lapacke_csp_trans.o \
      lapacke_cst_nancheck.o \
      lapacke_csy_nancheck.o \
      lapacke_csy_trans.o \
      lapacke_ctb_nancheck.o \
      lapacke_ctb_trans.o \
      lapacke_ctf_nancheck.o \
      lapacke_ctf_trans.o \
      lapacke_ctp_nancheck.o \
      lapacke_ctp_trans.o \
      lapacke_ctr_nancheck.o \
      lapacke_ctr_trans.o \
      lapacke_dgb_nancheck.o \
      lapacke_dgb_trans.o \
      lapacke_dge_nancheck.o \
      lapacke_dge_trans.o \
      lapacke_dgg_nancheck.o \
      lapacke_dgg_trans.o \
      lapacke_dgt_nancheck.o \
      lapacke_dhs_nancheck.o \
      lapacke_dhs_trans.o \
      lapacke_d_nancheck.o \
      lapacke_dpb_nancheck.o \
      lapacke_dpb_trans.o \
      lapacke_dpf_nancheck.o \
      lapacke_dpf_trans.o \
      lapacke_dpo_nancheck.o \
      lapacke_dpo_trans.o \
      lapacke_dpp_nancheck.o \
      lapacke_dpp_trans.o \
      lapacke_dpt_nancheck.o \
      lapacke_dsb_nancheck.o \
      lapacke_dsb_trans.o \
      lapacke_dsp_nancheck.o \
      lapacke_dsp_trans.o \
      lapacke_dst_nancheck.o \
      lapacke_dsy_nancheck.o \
      lapacke_dsy_trans.o \
      lapacke_dtb_nancheck.o \
      lapacke_dtb_trans.o \
      lapacke_dtf_nancheck.o \
      lapacke_dtf_trans.o \
      lapacke_dtp_nancheck.o \
      lapacke_dtp_trans.o \
      lapacke_dtr_nancheck.o \
      lapacke_dtr_trans.o \
      lapacke_lsame.o \
      lapacke_sgb_nancheck.o \
      lapacke_sgb_trans.o \
      lapacke_sge_nancheck.o \
      lapacke_sge_trans.o \
      lapacke_sgg_nancheck.o \
      lapacke_sgg_trans.o \
      lapacke_sgt_nancheck.o \
      lapacke_shs_nancheck.o \
      lapacke_shs_trans.o \
      lapacke_s_nancheck.o \
      lapacke_spb_nancheck.o \
      lapacke_spb_trans.o \
      lapacke_spf_nancheck.o \
      lapacke_spf_trans.o \
      lapacke_spo_nancheck.o \
      lapacke_spo_trans.o \
      lapacke_spp_nancheck.o \
      lapacke_spp_trans.o \
      lapacke_spt_nancheck.o \
      lapacke_ssb_nancheck.o \
      lapacke_ssb_trans.o \
      lapacke_ssp_nancheck.o \
      lapacke_ssp_trans.o \
      lapacke_sst_nancheck.o \
      lapacke_ssy_nancheck.o \
      lapacke_ssy_trans.o \
      lapacke_stb_nancheck.o \
      lapacke_stb_trans.o \
      lapacke_stf_nancheck.o \
      lapacke_stf_trans.o \
      lapacke_stp_nancheck.o \
      lapacke_stp_trans.o \
      lapacke_str_nancheck.o \
      lapacke_str_trans.o \
      lapacke_xerbla.o \
      lapacke_zgb_nancheck.o \
      lapacke_zgb_trans.o \
      lapacke_zge_nancheck.o \
      lapacke_zge_trans.o \
      lapacke_zgg_nancheck.o \
      lapacke_zgg_trans.o \
      lapacke_zgt_nancheck.o \
      lapacke_zhb_nancheck.o \
      lapacke_zhb_trans.o \
      lapacke_zhe_nancheck.o \
      lapacke_zhe_trans.o \
      lapacke_zhp_nancheck.o \
      lapacke_zhp_trans.o \
      lapacke_zhs_nancheck.o \
      lapacke_zhs_trans.o \
      lapacke_z_nancheck.o \
      lapacke_zpb_nancheck.o \
      lapacke_zpb_trans.o \
      lapacke_zpf_nancheck.o \
      lapacke_zpf_trans.o \
      lapacke_zpo_nancheck.o \
      lapacke_zpo_trans.o \
      lapacke_zpp_nancheck.o \
      lapacke_zpp_trans.o \
      lapacke_zpt_nancheck.o \
      lapacke_zsp_nancheck.o \
      lapacke_zsp_trans.o \
      lapacke_zst_nancheck.o \
      lapacke_zsy_nancheck.o \
      lapacke_zsy_trans.o \
      lapacke_ztb_nancheck.o \
      lapacke_ztb_trans.o \
      lapacke_ztf_nancheck.o \
      lapacke_ztf_trans.o \
      lapacke_ztp_nancheck.o \
      lapacke_ztp_trans.o \
      lapacke_ztr_nancheck.o \
      lapacke_ztr_trans.o \
      lapacke_make_complex_float.o \
      lapacke_make_complex_double.o

all: lib

lib: $(OBJ)
	$(ARCH) $(ARCHFLAGS) ../../$(LAPACKELIB) $^
	$(RANLIB) ../../$(LAPACKELIB)

clean: cleanobj
cleanobj:
	rm -f *.o

.c.o:
	$(CC) $(CFLAGS) -I../include -c -o $@ $<

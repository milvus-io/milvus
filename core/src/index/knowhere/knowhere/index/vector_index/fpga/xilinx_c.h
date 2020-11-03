// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#ifndef XILINX_C_H
#define XILINX_C_H

#include <assert.h>
#include <byteswap.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#define DEVICE_C2H0 "/dev/xdma0_c2h_0"
#define DEVICE_H2C0 "/dev/xdma0_h2c_0"
#define DEVICE_CTRL "/dev/xdma0_bypass"

// extern "C"{
// c2h0 functions
char*
TransFPGA2Mem(char* devicename, loff_t addr, size_t size, char* buffer, int count);
int
WriteMem2File(char* buffer, size_t size, int count, const char* filename);
int
DMAFPGA2File(char* devicename, loff_t addr, size_t size, size_t offset, int count, const char* filename);

// h2c0 functions
char*
ReadFile2Mem(const char* filename, char* buffer, size_t size);
int
TransBuffer2FPGA(char* buffer, size_t size, char* devicename, loff_t addr, int count);
int
DMAFile2FPGA(char* devicename, loff_t addr, size_t size, size_t offset, int count, const char* filename);

// register functions
int
open_dev(char* devicename);
int
close_dev(int fd);
int
reg_rw(int fd, loff_t addr, char* rw_mode, char access_mode, uint32_t data);
//}

#endif

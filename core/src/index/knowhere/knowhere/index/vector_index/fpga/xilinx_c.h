#ifndef XILINX_C_H
#define XILINX_C_H

#include <assert.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <byteswap.h>
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#include <termios.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define DEVICE_C2H0     "/dev/xdma/card0/c2h0"
#define DEVICE_H2C0     "/dev/xdma/card0/h2c0"
#define DEVICE_CTRL     "/dev/xdma0_bypass"

//extern "C"{
    // c2h0 functions
    char *TransFPGA2Mem(char *devicename, loff_t addr, size_t size, char *buffer, int count);
    int WriteMem2File(char *buffer, size_t size, int count, const char *filename);
    int DMAFPGA2File(char *devicename, loff_t addr, size_t size, size_t offset, int count, const char *filename);

    // h2c0 functions
    char *ReadFile2Mem(const char *filename, char *buffer, size_t size);
    int TransBuffer2FPGA(char *buffer, size_t size, char *devicename, loff_t addr, int count);
    int DMAFile2FPGA(char *devicename, loff_t addr, size_t size, size_t offset, int count, const char *filename);

    // register functions
    int open_dev(char *devicename);
    int close_dev(int fd);
    int reg_rw(int fd, loff_t addr, char* rw_mode, char access_mode, uint32_t data);
//}

#endif

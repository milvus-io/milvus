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

#include "knowhere/index/vector_index/fpga/xilinx_c.h"

/* ltoh: little to host */
/* htol: little to host */
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define ltohl(x) (x)
#define ltohs(x) (x)
#define htoll(x) (x)
#define htols(x) (x)
#elif __BYTE_ORDER == __BIG_ENDIAN
#define ltohl(x) __bswap_32(x)
#define ltohs(x) __bswap_16(x)
#define htoll(x) __bswap_32(x)
#define htols(x) __bswap_16(x)
#endif

#define FATAL                                                                                                 \
    do {                                                                                                      \
        fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", __LINE__, __FILE__, errno, strerror(errno)); \
        exit(1);                                                                                              \
    } while (0)

#define MAP_SIZE (4 * 1024UL)
#define MAP_MASK (MAP_SIZE - 1)

static int no_write = 0;

/* Subtract timespec t2 from t1
 *
 * Both t1 and t2 must already be normalized
 * i.e. 0 <= nsec < 1000000000 */
static void
timespec_sub(struct timespec* t1, const struct timespec* t2) {
    assert(t1->tv_nsec >= 0);
    assert(t1->tv_nsec < 1000000000);
    assert(t2->tv_nsec >= 0);
    assert(t2->tv_nsec < 1000000000);
    t1->tv_sec -= t2->tv_sec;
    t1->tv_nsec -= t2->tv_nsec;
    if (t1->tv_nsec >= 1000000000) {
        t1->tv_sec++;
        t1->tv_nsec -= 1000000000;
    } else if (t1->tv_nsec < 0) {
        t1->tv_sec--;
        t1->tv_nsec += 1000000000;
    }
}

char*
TransFPGA2Mem(char* devicename, loff_t addr, size_t size, char* buffer, int count) {
    int rc;
    // char *buffer = NULL;
    // char *allocated = NULL;
    // struct timespec ts_start, ts_end;

    // posix_memalign((void **)&allocated, 4096/*alignment*/, size + 4096);
    // assert(allocated);
    // buffer = allocated + offset;
    // printf("host memory buffer = %p\n", buffer);

    int fpga_fd = open(devicename, O_RDWR | O_NONBLOCK);
    assert(fpga_fd >= 0);

    while (count--) {
        memset(buffer, 0x00, size);

        // rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
        // select AXI MM address
        off_t off = lseek(fpga_fd, addr, SEEK_SET);
        rc = read(fpga_fd, buffer, size);
        if ((rc > 0) && (rc < size)) {
            printf("Short read of %d bytes into a %ld bytes buffer, could be a packet read?\n", rc, size);
        }

        // rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
    }

    // subtract the start time from the end time
    // timespec_sub(&ts_end, &ts_start);
    // display passed time, a bit less accurate but side-effects are accounted for
    // printf("CLOCK_MONOTONIC reports %ld.%09ld seconds (total) for last transfer of %d bytes\n", ts_end.tv_sec,
    // ts_end.tv_nsec, size);

    close(fpga_fd);

    return buffer;
}

int
WriteMem2File(char* buffer, size_t size, int count, const char* filename) {
    int rc;
    int file_fd = -1;
    // char *buffer = (char *)addr;
    // char *allocated = buffer - offset;

    if (filename) {
        file_fd = open(filename, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, 0666);
        assert(file_fd >= 0);
    }

    while (count--) {
        if ((file_fd >= 0) & (no_write == 0)) {
            // write buffer to file
            rc = write(file_fd, buffer, size);
            assert(rc == size);
        }
    }

    if (file_fd >= 0) {
        close(file_fd);
    }
    // free(allocated);
}

int
DMAFPGA2File(char* devicename, loff_t addr, size_t size, size_t offset, int count, const char* filename) {
    int rc;
    char* buffer = NULL;
    char* allocated = NULL;
    struct timespec ts_start, ts_end;

    printf("device = %s, address = 0x%lx, size = 0x%lx, offset = 0x%lx, count = %d\n", devicename, addr, size, offset,
           count);

    posix_memalign((void**)&allocated, 4096 /*alignment*/, size + 4096);
    assert(allocated);
    buffer = allocated + offset;
    printf("host memory buffer = %p\n", buffer);

    int file_fd = -1;
    int fpga_fd = open(devicename, O_RDWR | O_NONBLOCK);
    assert(fpga_fd >= 0);

    /* create file to write data to */
    if (filename) {
        file_fd = open(filename, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, 0666);
        assert(file_fd >= 0);
    }

    while (count--) {
        memset(buffer, 0x00, size);
        /* select AXI MM address */
        off_t off = lseek(fpga_fd, addr, SEEK_SET);
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
        /* read data from AXI MM into buffer using SGDMA */
        rc = read(fpga_fd, buffer, size);
        if ((rc > 0) && (rc < size)) {
            printf("Short read of %d bytes into a %ld bytes buffer, could be a packet read?\n", rc, size);
        }
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
        /* file argument given? */
        if ((file_fd >= 0) & (no_write == 0)) {
            /* write buffer to file */
            rc = write(file_fd, buffer, size);
            assert(rc == size);
        }
    }
    /* subtract the start time from the end time */
    // timespec_sub(&ts_end, &ts_start);
    /* display passed time, a bit less accurate but side-effects are accounted for */
    // printf("CLOCK_MONOTONIC reports %ld.%09ld seconds (total) for last transfer of %ld bytes\n", ts_end.tv_sec,
    // ts_end.tv_nsec, size);

    close(fpga_fd);
    if (file_fd >= 0) {
        close(file_fd);
    }
    free(allocated);
}

char*
ReadFile2Mem(const char* filename, char* buffer, size_t size) {
    int rc;
    // char *buffer = NULL;
    // char *allocated = NULL;
    int file_fd = -1;

    // allocate dst buffer in memory
    // posix_memalign((void **)&allocated, 4096/*alignment*/, size + 4096);
    // assert(allocated);
    // buffer = allocated + offset;
    // printf("host memory buffer = %p\n", buffer);

    if (filename) {
        file_fd = open(filename, O_RDONLY);
        assert(file_fd >= 0);
    }

    if (file_fd >= 0) {
        rc = read(file_fd, buffer, size);
        if (rc != size)
            perror("read(file_fd)");
        assert(rc == size);
    }

    if (file_fd >= 0) {
        close(file_fd);
    }
    // free(allocated);

    return buffer;
}

int
TransBuffer2FPGA(char* buffer, size_t size, char* devicename, loff_t addr, int count) {
    int rc;
    // struct timespec ts_start, ts_end;

    int fpga_fd = open(devicename, O_RDWR);
    assert(fpga_fd >= 0);

    // select AXI MM address
    off_t off = lseek(fpga_fd, addr, SEEK_SET);
    while (count--) {
        // rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
        /* write buffer to AXI MM address using SGDMA */
        rc = write(fpga_fd, buffer, size);
        assert(rc == size);
        // rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
    }

    /* subtract the start time from the end time */
    // timespec_sub(&ts_end, &ts_start);
    /* display passed time, a bit less accurate but side-effects are accounted for */
    // printf("CLOCK_MONOTONIC reports %ld.%09ld seconds (total) for last transfer of %lu bytes\n", ts_end.tv_sec,
    // ts_end.tv_nsec, size);

    close(fpga_fd);
}

int
DMAFile2FPGA(char* devicename, loff_t addr, size_t size, size_t offset, int count, const char* filename) {
    int rc;
    char* buffer = NULL;
    char* allocated = NULL;
    struct timespec ts_start, ts_end;

    posix_memalign((void**)&allocated, 4096 /*alignment*/, size + 4096);
    assert(allocated);
    buffer = allocated + offset;
    printf("host memory buffer = %p\n", buffer);

    int file_fd = -1;
    int fpga_fd = open(devicename, O_RDWR);
    assert(fpga_fd >= 0);

    if (filename) {
        file_fd = open(filename, O_RDONLY);
        assert(file_fd >= 0);
    }
    /* fill the buffer with data from file? */
    if (file_fd >= 0) {
        /* read data from file into memory buffer */
        rc = read(file_fd, buffer, size);
        if (rc != size)
            perror("read(file_fd)");
        assert(rc == size);
    }
    /* select AXI MM address */
    off_t off = lseek(fpga_fd, addr, SEEK_SET);
    while (count--) {
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
        /* write buffer to AXI MM address using SGDMA */
        rc = write(fpga_fd, buffer, size);
        assert(rc == size);
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
    }
    /* subtract the start time from the end time */
    timespec_sub(&ts_end, &ts_start);
    /* display passed time, a bit less accurate but side-effects are accounted for */
    printf("CLOCK_MONOTONIC reports %ld.%09ld seconds (total) for last transfer of %ld bytes\n", ts_end.tv_sec,
           ts_end.tv_nsec, size);

    close(fpga_fd);
    if (file_fd >= 0) {
        close(file_fd);
    }
    free(allocated);
}

int
open_dev(char* devicename) {
    int fd;
    char* device;
    device = devicename;
    if ((fd = open(device, O_RDWR | O_SYNC)) == -1)
        FATAL;
    else
        return fd;
}

int
close_dev(int fd) {
    close(fd);
    return 0;
}

int
reg_rw(int fd, loff_t addr, char* rw_mode, char access_mode, uint32_t data) {
    //  int fd;
    void *map_base, *virt_addr;
    uint32_t read_result, writeval;
    off_t target;
    int access_width = 'w';
    char* access_type;

    target = addr;

    access_type = rw_mode;
    access_width = access_mode;

    /* map one page */
    map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, target / 4096 * 4096);
    if (map_base == (void*)-1)
        FATAL;

    /* calculate the virtual address to be accessed */
    virt_addr = (void*)((uint8_t*)map_base + target % 4096);
    /* read only */

    if (strcmp(access_type, "read") == 0) {
        // printf("Read from address %p.\n", virt_addr);
        switch (access_width) {
            case 'b':
                read_result = *((uint8_t*)virt_addr);
                // printf("Read 8-bits value at address 0x%08x (%p): 0x%02x\n", (unsigned int)target, virt_addr,
                // (unsigned int)read_result);
                break;
            case 'h':
                read_result = *((uint16_t*)virt_addr);
                /* swap 16-bit endianess if host is not little-endian */
                read_result = ltohs(read_result);
                // printf("Read 16-bit value at address 0x%08x (%p): 0x%04x\n", (unsigned int)target, virt_addr,
                // (unsigned int)read_result);
                break;
            case 'w':
                read_result = *((uint32_t*)virt_addr);
                /* swap 32-bit endianess if host is not little-endian */
                read_result = ltohl(read_result);
                // printf("Read 32-bit value at address 0x%08x (%p): 0x%08x\n", (unsigned int)target, virt_addr,
                // (unsigned int)read_result);
                return (int)read_result;
                break;
            default:
                fprintf(stderr, "Illegal data type '%c'.\n", access_width);
                exit(2);
        }
        fflush(stdout);
    }
    /* data value given, i.e. writing? */
    if (strcmp(access_type, "write") == 0) {
        writeval = data;
        switch (access_width) {
            case 'b':
                // printf("Write 8-bits value 0x%02x to 0x%08x (0x%p)\n", (unsigned int)writeval, (unsigned int)target,
                // virt_addr);
                *((uint8_t*)virt_addr) = writeval;
                break;
            case 'h':
                // printf("Write 16-bits value 0x%04x to 0x%08x (0x%p)\n", (unsigned int)writeval, (unsigned int)target,
                // virt_addr);
                /* swap 16-bit endianess if host is not little-endian */
                writeval = htols(writeval);
                *((uint16_t*)virt_addr) = writeval;
                break;
            case 'w':
                // printf("Write 32-bits value 0x%08x to 0x%08x (0x%p)\n", (unsigned int)writeval, (unsigned int)target,
                // virt_addr);
                /* swap 32-bit endianess if host is not little-endian */
                writeval = htoll(writeval);
                *((uint32_t*)virt_addr) = writeval;
                break;
        }
        fflush(stdout);
    }
    if (munmap(map_base, MAP_SIZE) == -1)
        FATAL;
    // close(fd);
    return 0;
}

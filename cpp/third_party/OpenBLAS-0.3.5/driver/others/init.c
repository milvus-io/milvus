/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of 
      its contributors may be used to endorse or promote products 
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
kOR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/


/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include "common.h"

#if defined(OS_LINUX) && defined(SMP)

#define _GNU_SOURCE

#include <sys/sysinfo.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/shm.h>
#include <fcntl.h>
#include <sched.h>
#include <dirent.h>
#include <dlfcn.h>
#include <unistd.h>
#include <string.h>

#if defined(BIGNUMA)
// max number of nodes as defined in numa.h
// max cpus as defined in most sched.h
// cannot use CPU_SETSIZE directly as some
// Linux distributors set it to 4096 
#define MAX_NODES	128
#define MAX_CPUS	1024
#else
#define MAX_NODES	16
#define MAX_CPUS	256
#endif

#define NCPUBITS        (8*sizeof(unsigned long))
#define MAX_BITMASK_LEN (MAX_CPUS/NCPUBITS)
#define CPUELT(cpu)	((cpu) / NCPUBITS)
#define CPUMASK(cpu)	((unsigned long) 1UL << ((cpu) % NCPUBITS))


#define SH_MAGIC	0x510510

#define CPUMAP_NAME	"/sys/devices/system/node/node%d/cpumap"
#define SHARE_NAME	"/sys/devices/system/cpu/cpu%d/cache/index%d/shared_cpu_map"
#define NODE_DIR	"/sys/devices/system/node"

//#undef DEBUG

/* Private variables */
typedef struct {
  unsigned long lock;
  unsigned int magic;
  unsigned int shmid;

  int num_nodes;
  int num_procs;
  int final_num_procs;
  unsigned long avail [MAX_BITMASK_LEN];
  int avail_count;
  unsigned long cpu_info   [MAX_CPUS];
  unsigned long node_info  [MAX_NODES][MAX_BITMASK_LEN];
  int cpu_use[MAX_CPUS];

} shm_t;

static cpu_set_t cpu_orig_mask[4];

static int  cpu_mapping[MAX_CPUS];
static int  node_mapping[MAX_CPUS * 4];
static int  cpu_sub_mapping[MAX_CPUS];
static int  disable_mapping;

/* Number of cores per nodes */
static int  node_cpu[MAX_NODES];
static int  node_equal = 0;

static shm_t *common = (void *)-1;
static int shmid, pshmid;
static void *paddr;

static unsigned long lprocmask[MAX_BITMASK_LEN], lnodemask;
static int lprocmask_count = 0;
static int numprocs = 1;
static int numnodes = 1;

#if 1
#define READ_CPU(x)   ( (x)        & 0xff)
#define READ_NODE(x)  (((x) >>  8) & 0xff)
#define READ_CORE(x)  (((x) >> 16) & 0xff)

#define WRITE_CPU(x)    (x)
#define WRITE_NODE(x)  ((x) <<  8)
#define WRITE_CORE(x)  ((x) << 16)
#else
#define READ_CPU(x)   ( (x)        & 0xff)
#define READ_CORE(x)  (((x) >>  8) & 0xff)
#define READ_NODE(x)  (((x) >> 16) & 0xff)

#define WRITE_CPU(x)    (x)
#define WRITE_CORE(x)  ((x) <<  8)
#define WRITE_NODE(x)  ((x) << 16)
#endif

static inline int popcount(unsigned long number) {

  int count = 0;

  while (number > 0) {
    if (number & 1) count ++;
    number >>= 1;
  }

  return count;
}

static inline int rcount(unsigned long number) {

  int count = -1;

  while ((number > 0) && ((number & 0)) == 0) {
    count ++;
    number >>= 1;
  }

  return count;
}

/***
  Known issue: The number of CPUs/cores should less
  than sizeof(unsigned long). On 64 bits, the limit
  is 64. On 32 bits, it is 32.
***/
static inline void get_cpumap(int node, unsigned long * node_info) {

  int infile;
  unsigned long affinity[32];
  char name[160];
  char cpumap[160];
  char *dummy;
  int i=0;
  int count=0;
  int k=0;

  sprintf(name, CPUMAP_NAME, node);

  infile = open(name, O_RDONLY);
  for(i=0; i<32; i++){
    affinity[i] = 0;
  }

  if (infile != -1) {

    read(infile, cpumap, sizeof(cpumap));

    for(i=0; i<160; i++){
      if(cpumap[i] == '\n')
	break;
      if(cpumap[i] != ','){
	name[k++]=cpumap[i];

	//Enough data for Hex
	if(k >= NCPUBITS/4){
	  affinity[count++] = strtoul(name, &dummy, 16);
	  k=0;
	}
      }

    }
    if(k!=0){
      name[k]='\0';
      affinity[count++] = strtoul(name, &dummy, 16);
      // k=0;
    }
    // 0-63bit -> node_info[0], 64-128bit -> node_info[1] ....
    // revert the sequence
    for(i=0; i<count && i<MAX_BITMASK_LEN; i++){
      node_info[i]=affinity[count-i-1];
    }
    close(infile);
  }

  return ;
}

static inline void get_share(int cpu, int level, unsigned long * share) {

  int infile;
  unsigned long affinity[32];
  char cpumap[160];
  char name[160];
  char *dummy;
  int count=0;
  int i=0,k=0;
  int bitmask_idx = 0;

  sprintf(name, SHARE_NAME, cpu, level);

  infile = open(name, O_RDONLY);

  //  Init share
  for(i=0; i<MAX_BITMASK_LEN; i++){
    share[i]=0;
  }
  bitmask_idx = CPUELT(cpu);
  share[bitmask_idx] = CPUMASK(cpu);

  if (infile != -1) {

    read(infile, cpumap, sizeof(cpumap));

    for(i=0; i<160; i++){
      if(cpumap[i] == '\n')
	break;
      if(cpumap[i] != ','){
	name[k++]=cpumap[i];

	//Enough data
	if(k >= NCPUBITS/4){
	  affinity[count++] = strtoul(name, &dummy, 16);
	  k=0;
	}
      }

    }
    if(k!=0){
      name[k]='\0';
      affinity[count++] = strtoul(name, &dummy, 16);
      // k=0;
    }
    // 0-63bit -> node_info[0], 64-128bit -> node_info[1] ....
    // revert the sequence
    for(i=0; i<count && i<MAX_BITMASK_LEN; i++){
      share[i]=affinity[count-i-1];
    }


    close(infile);
  }

  return ;
}

static int numa_check(void) {

  DIR *dp;
  struct dirent *dir;
  int node;
  int j;

  common -> num_nodes = 0;

  dp = opendir(NODE_DIR);

  if (dp == NULL) {
    common -> num_nodes = 1;
    return 0;
  }

  for (node = 0; node < MAX_NODES; node ++) {
    for (j = 0; j<MAX_BITMASK_LEN; j++) common -> node_info[node][j] = 0;
  }

  while ((dir = readdir(dp)) != NULL) {
    if (strncmp(dir->d_name, "node", 4)==0) {

      node = atoi(&dir -> d_name[4]);

      if (node > MAX_NODES) {
	fprintf(stderr, "\nOpenBLAS Warning : MAX_NODES (NUMA) is too small. Terminated.\n");
	exit(1);
      }

      common -> num_nodes ++;
      get_cpumap(node, common->node_info[node]);

    }
  }

  closedir(dp);

  if (common -> num_nodes == 1) return 1;

#ifdef DEBUG
  fprintf(stderr, "Numa found : number of Nodes = %2d\n", common -> num_nodes);

  for (node = 0; node < common -> num_nodes; node ++)
    fprintf(stderr, "MASK (%2d) : %08lx\n", node, common -> node_info[node][0]);
#endif

  return common -> num_nodes;
}

#if defined(__GLIBC_PREREQ)
#if !__GLIBC_PREREQ(2, 6)
int sched_getcpu(void)
{
int cpu;
FILE *fp = NULL;
if ( (fp = fopen("/proc/self/stat", "r")) == NULL)
   return -1;
if ( fscanf( fp, "%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%d", &cpu) != 1) {
  fclose (fp);
  return -1;
  }
  fclose (fp);
  return(cpu);
}
#endif
#endif

static void numa_mapping(void) {

  int node, cpu, core;
  int i, j, h;
  unsigned long work, bit;
  int count = 0;
  int bitmask_idx = 0;
  int current_cpu;
  int current_node = 0;
  int cpu_count = 0;

  for (node = 0; node < common -> num_nodes; node ++) {
    core = 0;
    for (cpu = 0; cpu < common -> num_procs; cpu ++) {
      bitmask_idx = CPUELT(cpu);
      if (common -> node_info[node][bitmask_idx] & common -> avail[bitmask_idx] & CPUMASK(cpu)) {
	common -> cpu_info[count] = WRITE_CORE(core) | WRITE_NODE(node) | WRITE_CPU(cpu);
	count ++;
	core ++;
      }

    }
  }

#ifdef DEBUG
  fprintf(stderr, "\nFrom /sys ...\n\n");

  for (cpu = 0; cpu < count; cpu++)
    fprintf(stderr, "CPU (%2d) : %08lx\n", cpu, common -> cpu_info[cpu]);
#endif

  current_cpu = sched_getcpu();
  for (cpu = 0; cpu < count; cpu++) {
    if (READ_CPU(common -> cpu_info[cpu]) == current_cpu) {
      current_node = READ_NODE(common -> cpu_info[cpu]);
      break;
    }
  }
  for (i = 0; i < MAX_BITMASK_LEN; i++)
    cpu_count += popcount(common -> node_info[current_node][i] & common -> avail[i]);

  /*
   * If all the processes can be accommodated in the
   * in the current node itself, then bind to cores
   * from the current node only
   */
  if (numprocs <= cpu_count) {
    /*
     * First sort all the cores in order from the current node.
     * Then take remaining nodes one by one in order,
     * and sort their cores in order.
     */
    for (i = 0; i < count; i++) {
      for (j = 0; j < count - 1; j++) {
        int node_1, node_2;
        int core_1, core_2;
        int swap = 0;

        node_1 = READ_NODE(common -> cpu_info[j]);
        node_2 = READ_NODE(common -> cpu_info[j + 1]);
        core_1 = READ_CORE(common -> cpu_info[j]);
        core_2 = READ_CORE(common -> cpu_info[j + 1]);

        if (node_1 == node_2) {
          if (core_1 > core_2)
            swap = 1;
        } else {
          if ((node_2 == current_node) ||
              ((node_1 != current_node) && (node_1 > node_2)))
            swap = 1;
        }
        if (swap) {
          unsigned long temp;

          temp = common->cpu_info[j];
          common->cpu_info[j] = common->cpu_info[j + 1];
          common->cpu_info[j + 1] = temp;
        }
      }
    }
  } else {
    h = 1;

    while (h < count) h = 2 * h + 1;

    while (h > 1) {
      h /= 2;
      for (i = h; i < count; i++) {
        work = common -> cpu_info[i];
        bit  = CPU_ISSET(i, &cpu_orig_mask[0]);
        j = i - h;
        while (work < common -> cpu_info[j]) {
          common -> cpu_info[j + h] = common -> cpu_info[j];
          if (CPU_ISSET(j, &cpu_orig_mask[0])) {
            CPU_SET(j + h, &cpu_orig_mask[0]);
          } else {
            CPU_CLR(j + h, &cpu_orig_mask[0]);
          }
          j -= h;
          if (j < 0) break;
        }
        common -> cpu_info[j + h] = work;
        if (bit) {
          CPU_SET(j + h, &cpu_orig_mask[0]);
        } else {
          CPU_CLR(j + h, &cpu_orig_mask[0]);
        }

      }
    }
  }

#ifdef DEBUG
  fprintf(stderr, "\nSorting ...\n\n");

  for (cpu = 0; cpu < count; cpu++)
    fprintf(stderr, "CPUINFO (%2d) : %08lx (CPU=%3lu CORE=%3lu NODE=%3lu)\n", cpu, common -> cpu_info[cpu],
      READ_CPU(common -> cpu_info[cpu]),
      READ_CORE(common -> cpu_info[cpu]),
      READ_NODE(common -> cpu_info[cpu]));
#endif

}

static void disable_hyperthread(void) {

  unsigned long share[MAX_BITMASK_LEN];
  int cpu;
  int bitmask_idx = 0;
  int i=0, count=0;
  bitmask_idx = CPUELT(common -> num_procs);

  for(i=0; i< bitmask_idx; i++){
    common -> avail[count++] = 0xFFFFFFFFFFFFFFFFUL;
  }
  if(CPUMASK(common -> num_procs) != 1){
    common -> avail[count++] = CPUMASK(common -> num_procs) - 1;
  }
  common -> avail_count = count;

  /* if(common->num_procs > 64){ */
  /*   fprintf(stderr, "\nOpenBLAS Warning : The number of CPU/Cores(%d) is beyond the limit(64). Terminated.\n", common->num_procs); */
  /*   exit(1); */
  /* }else if(common->num_procs == 64){ */
  /*   common -> avail = 0xFFFFFFFFFFFFFFFFUL; */
  /* }else */
  /*   common -> avail = (1UL << common -> num_procs) - 1; */

#ifdef DEBUG
  fprintf(stderr, "\nAvail CPUs    : ");
  for(i=0; i<count; i++)
    fprintf(stderr, "%04lx ", common -> avail[i]);
  fprintf(stderr, ".\n");
#endif

  for (cpu = 0; cpu < common -> num_procs; cpu ++) {

    get_share(cpu, 1, share);

    //When the shared cpu are in different element of share & avail array, this may be a bug.
    for (i = 0; i < count ; i++){

      share[i] &= common->avail[i];

      if (popcount(share[i]) > 1) {

#ifdef DEBUG
	fprintf(stderr, "Detected Hyper Threading on CPU %4x; disabled CPU %04lx.\n",
		cpu, share[i] & ~(CPUMASK(cpu)));
#endif

	common -> avail[i] &= ~((share[i] & ~ CPUMASK(cpu)));
      }
    }
  }
}

static void disable_affinity(void) {
  int i=0;
  int bitmask_idx=0;
  int count=0;
#ifdef DEBUG
    fprintf(stderr, "Final all available CPUs  : %04lx.\n\n", common -> avail[0]);
    fprintf(stderr, "CPU mask                  : %04lx.\n\n", *(unsigned long *)&cpu_orig_mask[0]);
#endif

  /* if(common->final_num_procs > 64){ */
  /*   fprintf(stderr, "\nOpenBLAS Warining : The number of CPU/Cores(%d) is beyond the limit(64). Terminated.\n", common->final_num_procs); */
  /*   exit(1); */
  /* }else if(common->final_num_procs == 64){ */
  /*   lprocmask = 0xFFFFFFFFFFFFFFFFUL; */
  /* }else */
  /*   lprocmask = (1UL << common -> final_num_procs) - 1; */

  bitmask_idx = CPUELT(common -> final_num_procs);

  for(i=0; i< bitmask_idx; i++){
    lprocmask[count++] = 0xFFFFFFFFFFFFFFFFUL;
  }
  if(CPUMASK(common -> final_num_procs) != 1){
    lprocmask[count++] = CPUMASK(common -> final_num_procs) - 1;
  }
  lprocmask_count = count;

#ifndef USE_OPENMP
  for(i=0; i< count; i++){
    lprocmask[i] &= common->avail[i];
  }
#endif

#ifdef DEBUG
    fprintf(stderr, "I choose these CPUs  : %04lx.\n\n", lprocmask[0]);
#endif

}

static void setup_mempolicy(void) {

  int cpu, mynode, maxcpu;

  for (cpu = 0; cpu < MAX_NODES; cpu ++) node_cpu[cpu] = 0;

  maxcpu = 0;

  for (cpu = 0; cpu < numprocs; cpu ++) {
    mynode = READ_NODE(common -> cpu_info[cpu_sub_mapping[cpu]]);

    lnodemask |= (1UL << mynode);

    node_cpu[mynode] ++;

    if (maxcpu < node_cpu[mynode]) maxcpu = node_cpu[mynode];
  }

  node_equal = 1;

  for (cpu = 0; cpu < MAX_NODES; cpu ++) if ((node_cpu[cpu] != 0) && (node_cpu[cpu] != maxcpu)) node_equal = 0;

  if (lnodemask) {

#ifdef DEBUG
    fprintf(stderr, "Node mask = %lx\n", lnodemask);
#endif

    my_set_mempolicy(MPOL_INTERLEAVE, &lnodemask, sizeof(lnodemask) * 8);

    numnodes = popcount(lnodemask);
  }
}

static inline int is_dead(int id) {

  struct shmid_ds ds;

  return shmctl(id, IPC_STAT, &ds);
}

static int open_shmem(void) {

  int try = 0;

  int err = 0;
  
  do {

#if defined(BIGNUMA)
    // raised to 32768, enough for 128 nodes and 1024 cups
    shmid = shmget(SH_MAGIC, 32768, 0666);
#else
    shmid = shmget(SH_MAGIC, 4096, 0666);
#endif

    if (shmid == -1) {
#if defined(BIGNUMA)
      shmid = shmget(SH_MAGIC, 32768, IPC_CREAT | 0666);
#else
      shmid = shmget(SH_MAGIC, 4096, IPC_CREAT | 0666);
#endif
    }

    if (shmid == -1) err = errno;
    
    try ++;

  } while ((try < 10) && (shmid == -1));

  if (shmid == -1) {
    fprintf (stderr, "Obtaining shared memory segment failed in open_shmem: %s\n",strerror(err));
    fprintf (stderr, "Setting CPU affinity not possible without shared memory access.\n");
    return (1);
  }

  if (shmid != -1) {
    if ( (common = shmat(shmid, NULL, 0)) == (void*)-1) {
      perror ("Attaching shared memory segment failed in open_shmem");
      fprintf (stderr, "Setting CPU affinity not possible without shared memory access.\n");
      return (1);
    }
  }
#ifdef DEBUG
  fprintf(stderr, "Shared Memory id = %x  Address = %p\n", shmid, common);
#endif
  return (0);
}

static int create_pshmem(void) {

  pshmid = shmget(IPC_PRIVATE, 4096, IPC_CREAT | 0666);

  if (pshmid == -1) {
    perror ("Obtaining shared memory segment failed in create_pshmem");
    fprintf (stderr, "Setting CPU affinity not possible without shared memory access.\n");
    return(1);
  }
  
  if ( (paddr = shmat(pshmid, NULL, 0)) == (void*)-1) {
    perror ("Attaching shared memory segment failed in create_pshmem");
    fprintf (stderr, "Setting CPU affinity not possible without shared memory access.\n");
    return (1);
  }  
  
  if (shmctl(pshmid, IPC_RMID, 0) == -1) return (1);

#ifdef DEBUG
  fprintf(stderr, "Private Shared Memory id = %x  Address = %p\n", pshmid, paddr);
#endif
  return(0);
}

static void local_cpu_map(void) {

  int cpu, id, mapping;
  int bitmask_idx = 0;
  cpu = 0;
  mapping = 0;

  do {
    id   = common -> cpu_use[cpu];

    if (id > 0) {
      if (is_dead(id)) common -> cpu_use[cpu] = 0;
    }

    bitmask_idx = CPUELT(cpu);
    if ((common -> cpu_use[cpu] == 0) && (lprocmask[bitmask_idx] & CPUMASK(cpu))) {

      common -> cpu_use[cpu] = pshmid;
      cpu_mapping[mapping] = READ_CPU(common -> cpu_info[cpu]);
      cpu_sub_mapping[mapping] = cpu;

      mapping ++;
    }

    cpu ++;

  } while ((mapping < numprocs) && (cpu < common -> final_num_procs));

  disable_mapping = 0;

  if ((mapping < numprocs) || (numprocs == 1)) {
    for (cpu = 0; cpu < common -> final_num_procs; cpu ++) {
      if (common -> cpu_use[cpu] == pshmid) common -> cpu_use[cpu] = 0;
    }
    disable_mapping = 1;
  }

#ifdef DEBUG
  for (cpu = 0; cpu < numprocs; cpu ++) {
    fprintf(stderr, "Local Mapping  : %2d --> %2d (%2d)\n", cpu, cpu_mapping[cpu], cpu_sub_mapping[cpu]);
  }
#endif
}

/* Public Functions */

int get_num_procs(void)  { return numprocs; }
int get_num_nodes(void)  { return numnodes; }
int get_node_equal(void) {

  return (((blas_cpu_number % numnodes) == 0) && node_equal);

}

int gotoblas_set_affinity(int pos) {

  cpu_set_t cpu_mask;

  int mynode = 1;

  /* if number of threads is larger than inital condition */
  if (pos < 0) {
      sched_setaffinity(0, sizeof(cpu_orig_mask), &cpu_orig_mask[0]);
      return 0;
  }

  if (!disable_mapping) {

    mynode = READ_NODE(common -> cpu_info[cpu_sub_mapping[pos]]);

#ifdef DEBUG
    fprintf(stderr, "Giving Affinity[%4d   %3d] --> %3d  My node = %3d\n", getpid(), pos, cpu_mapping[pos], mynode);
#endif

    CPU_ZERO(&cpu_mask);
    CPU_SET (cpu_mapping[pos], &cpu_mask);

    sched_setaffinity(0, sizeof(cpu_mask), &cpu_mask);

    node_mapping[WhereAmI()] = mynode;

  }

  return mynode;
}

int get_node(void) {

  if (!disable_mapping) return node_mapping[WhereAmI()];

  return 1;
}

static int initialized = 0;

void gotoblas_affinity_init(void) {

  int cpu, num_avail;
#ifndef USE_OPENMP
  cpu_set_t cpu_mask;
#endif
  int i;

  if (initialized) return;

  initialized = 1;

  sched_getaffinity(0, sizeof(cpu_orig_mask), &cpu_orig_mask[0]);

#ifdef USE_OPENMP
  numprocs = 0;
#else
  numprocs = readenv_atoi("OPENBLAS_NUM_THREADS");
  if (numprocs == 0) numprocs = readenv_atoi("GOTO_NUM_THREADS");
#endif

  if (numprocs == 0) numprocs = readenv_atoi("OMP_NUM_THREADS");

  numnodes = 1;

  if (numprocs == 1) {
    disable_mapping = 1;
    return;
  }

  if (create_pshmem() != 0) {
    disable_mapping = 1;
    return;
  }
  
  if (open_shmem() != 0) {
    disable_mapping = 1;
    return;
  }
  
  while ((common -> lock) && (common -> magic != SH_MAGIC)) {
    if (is_dead(common -> shmid)) {
      common -> lock = 0;
      common -> shmid = 0;
      common -> magic = 0;
    } else {
      YIELDING;
    }
  }

  blas_lock(&common -> lock);

  if ((common -> shmid) && is_dead(common -> shmid)) common -> magic = 0;

  common -> shmid = pshmid;

  if (common -> magic != SH_MAGIC) {
    cpu_set_t *cpusetp;
    int nums;
    int ret;

#ifdef DEBUG
    fprintf(stderr, "Shared Memory Initialization.\n");
#endif

    //returns the number of processors which are currently online

    nums = sysconf(_SC_NPROCESSORS_CONF);

#if !defined(__GLIBC_PREREQ)
    common->num_procs = nums;
#else

#if !__GLIBC_PREREQ(2, 3)
    common->num_procs = nums;
#elif __GLIBC_PREREQ(2, 7)
    cpusetp = CPU_ALLOC(nums);
    if (cpusetp == NULL) {
        common->num_procs = nums;
    } else {
        size_t size;
        size = CPU_ALLOC_SIZE(nums);
        ret = sched_getaffinity(0,size,cpusetp);
        if (ret!=0)
            common->num_procs = nums;
        else
            common->num_procs = CPU_COUNT_S(size,cpusetp);
    }
    CPU_FREE(cpusetp);
#else
    ret = sched_getaffinity(0,sizeof(cpu_set_t), cpusetp);
    if (ret!=0) {
        common->num_procs = nums;
    } else {
#if !__GLIBC_PREREQ(2, 6)
    int i;
    int n = 0;
    for (i=0;i<nums;i++)
        if (CPU_ISSET(i,cpusetp)) n++;
    common->num_procs = n;
    }
#else
    common->num_procs = CPU_COUNT(sizeof(cpu_set_t),cpusetp);
    }
#endif

#endif
#endif
    if(common -> num_procs > MAX_CPUS) {
      fprintf(stderr, "\nOpenBLAS Warning : The number of CPU/Cores(%d) is beyond the limit(%d). Terminated.\n", common->num_procs, MAX_CPUS);
      exit(1);
    }

    for (cpu = 0; cpu < common -> num_procs; cpu++) common -> cpu_info[cpu] = cpu;

    numa_check();

    disable_hyperthread();

    if (common -> num_nodes > 1) numa_mapping();

    common -> final_num_procs = 0;
    for(i = 0; i < common -> avail_count; i++) common -> final_num_procs += rcount(common -> avail[i]) + 1;   //Make the max cpu number. 

    for (cpu = 0; cpu < common -> final_num_procs; cpu ++) common -> cpu_use[cpu] =  0;

    common -> magic = SH_MAGIC;

  }

  disable_affinity();

  num_avail = 0;
  for(i=0; i<lprocmask_count; i++) num_avail += popcount(lprocmask[i]);

  if ((numprocs <= 0) || (numprocs > num_avail)) numprocs = num_avail;

#ifdef DEBUG
  fprintf(stderr, "Number of threads = %d\n", numprocs);
#endif

  local_cpu_map();

  blas_unlock(&common -> lock);

#ifndef USE_OPENMP
  if (!disable_mapping) {

#ifdef DEBUG
    fprintf(stderr, "Giving Affinity[%3d] --> %3d\n", 0, cpu_mapping[0]);
#endif

    CPU_ZERO(&cpu_mask);
    CPU_SET (cpu_mapping[0], &cpu_mask);

    sched_setaffinity(0, sizeof(cpu_mask), &cpu_mask);

    node_mapping[WhereAmI()] = READ_NODE(common -> cpu_info[cpu_sub_mapping[0]]);

    setup_mempolicy();

    if (readenv_atoi("OPENBLAS_MAIN_FREE") || readenv_atoi("GOTOBLAS_MAIN_FREE")) {
      sched_setaffinity(0, sizeof(cpu_orig_mask), &cpu_orig_mask[0]);
    }

  }
#endif

#ifdef DEBUG
  fprintf(stderr, "Initialization is done.\n");
#endif
}

void gotoblas_affinity_quit(void) {

  int i;
  struct shmid_ds ds;

#ifdef DEBUG
  fprintf(stderr, "Terminating ..\n");
#endif

  if ((numprocs == 1) || (initialized == 0)) return;

  if (!disable_mapping) {

    blas_lock(&common -> lock);

    for (i = 0; i < numprocs; i ++) common -> cpu_use[cpu_mapping[i]] = -1;

    blas_unlock(&common -> lock);

  }

  shmctl(shmid, IPC_STAT, &ds);

  if (ds.shm_nattch == 1) shmctl(shmid, IPC_RMID, 0);

  shmdt(common);

  shmdt(paddr);

  initialized = 0;
}

#else

void gotoblas_affinity_init(void) {};

void gotoblas_set_affinity(int threads) {};

void gotoblas_set_affinity2(int threads) {};

void gotoblas_affinity_reschedule(void) {};

int get_num_procs(void) { return sysconf(_SC_NPROCESSORS_CONF); }

int get_num_nodes(void) { return 1; }

int get_node(void) { return 1;}
#endif



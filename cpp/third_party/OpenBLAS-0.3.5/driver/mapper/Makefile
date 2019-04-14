MODULENAME 	:= mapper

KDIR		:= /lib/modules/$(shell uname -r)/build
PWD		:= $(shell pwd)

CC		:= gcc -Wall

ifeq ($(KERNELRELEASE),)
all ::
	$(MAKE) -C $(KDIR) SUBDIRS=$(PWD) modules
else
  obj-m	:= $(MODULENAME).o
endif

load:
	insmod ./$(MODULENAME).ko

unload:
	rmmod $(MODULENAME)

setup:
	./device_setup

clean:
	rm -rf *.o *.ko Module.symvers *.mod.c .tmp_versions .mapper* modules.order

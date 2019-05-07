/*
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

/* DO_XXH should be 32 or 64 for xxh32 and xxh64 respectively */
#define DO_XXH 0
/* DO_CRC should be 0 or 1 */
#define DO_CRC 0
/* Buffer size */
#define BUFFER_SIZE 4096

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/fs.h>
#include <linux/uaccess.h>

#if DO_XXH
#include <linux/xxhash.h>
#endif

#if DO_CRC
#include <linux/crc32.h>
#endif

/* Device name to pass to register_chrdev(). */
#define DEVICE_NAME "xxhash_test"

/* Dynamically allocated device major number */
static int device_major;

/*
 * We reuse the same hash state, and thus can hash only one
 * file at a time.
 */
static bool device_is_open;

static uint64_t total_length;


#if (DO_XXH == 32)

#define xxh_state xxh32_state
#define xxh_reset xxh32_reset
#define xxh_update xxh32_update
#define xxh_digest xxh32_digest
#define XXH_FORMAT "XXH32 = 0x%x"

#elif (DO_XXH == 64)

#define xxh_state xxh64_state
#define xxh_reset xxh64_reset
#define xxh_update xxh64_update
#define xxh_digest xxh64_digest
#define XXH_FORMAT "XXH64 = 0x%llx"

#elif DO_XXH

#error "Invalid value of DO_XXH"

#endif

#if DO_XXH

/* XXH state */
static struct xxh_state state;

#endif /* DO_XXH */

#if DO_CRC

static uint32_t crc;

#endif /* DO_CRC */

/*
 * Input buffer used to put data coming from userspace.
 */
static uint8_t buffer_in[BUFFER_SIZE];

static int xxhash_test_open(struct inode *i, struct file *f)
{
	if (device_is_open)
		return -EBUSY;

	device_is_open = true;

	total_length = 0;
#if DO_XXH
	xxh_reset(&state, 0);
#endif
#if DO_CRC
	crc = 0xFFFFFFFF;
#endif

	printk(KERN_INFO DEVICE_NAME ": opened\n");
	return 0;
}

static int xxhash_test_release(struct inode *i, struct file *f)
{
	device_is_open = false;

	printk(KERN_INFO DEVICE_NAME ": total_len = %llu\n", total_length);
#if DO_XXH
	printk(KERN_INFO DEVICE_NAME ": " XXH_FORMAT "\n", xxh_digest(&state));
#endif
#if DO_CRC
	printk(KERN_INFO DEVICE_NAME ": CRC32 = 0x%08x\n", ~crc);
#endif
	printk(KERN_INFO DEVICE_NAME ": closed\n");
	return 0;
}

/*
 * Hash the data given to us from userspace.
 */
static ssize_t xxhash_test_write(struct file *file, const char __user *buf,
				 size_t size, loff_t *pos)
{
	size_t remaining = size;

	while (remaining > 0) {
#if DO_XXH
		int ret;
#endif
		size_t const copy_size = min(remaining, sizeof(buffer_in));

		if (copy_from_user(buffer_in, buf, copy_size))
			return -EFAULT;
		buf += copy_size;
		remaining -= copy_size;
		total_length += copy_size;
#if DO_XXH
		if ((ret = xxh_update(&state, buffer_in, copy_size))) {
			printk(KERN_INFO DEVICE_NAME ": xxh failure.");
			return ret;
		}
#endif
#if DO_CRC
		crc = crc32(crc, buffer_in, copy_size);
#endif
	}
	return size;
}
/* register the character device. */
static int __init xxhash_test_init(void)
{
	static const struct file_operations fileops = {
		.owner = THIS_MODULE,
		.open = &xxhash_test_open,
		.release = &xxhash_test_release,
		.write = &xxhash_test_write
	};

	device_major = register_chrdev(0, DEVICE_NAME, &fileops);
	if (device_major < 0) {
		return device_major;
	}

	printk(KERN_INFO DEVICE_NAME ": module loaded\n");
	printk(KERN_INFO DEVICE_NAME ": Create a device node with "
			"'mknod " DEVICE_NAME " c %d 0' and write data "
			"to it.\n", device_major);
	return 0;
}

static void __exit xxhash_test_exit(void)
{
	unregister_chrdev(device_major, DEVICE_NAME);
	printk(KERN_INFO DEVICE_NAME ": module unloaded\n");
}

module_init(xxhash_test_init);
module_exit(xxhash_test_exit);

MODULE_DESCRIPTION("XXHash tester");
MODULE_VERSION("1.0");


MODULE_LICENSE("Dual BSD/GPL");

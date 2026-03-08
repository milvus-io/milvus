//go:build use_asan
// +build use_asan

package asan

// void __lsan_do_leak_check(void);
import "C"

func LsanDoLeakCheck() {
	C.__lsan_do_leak_check()
}

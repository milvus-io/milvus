#ifndef LINUX_KERNEL_H_
#define LINUX_KERNEL_H_

#define ALIGN(x, a) ({                                                         \
    typeof(x) const __xe = (x);                                                \
    typeof(a) const __ae = (a);                                                \
    typeof(a) const __m = __ae - 1;                                            \
    typeof(x) const __r = __xe & __m;                                          \
    __xe + (__r ? (__ae - __r) : 0);                                           \
  })

#define PTR_ALIGN(p, a) (typeof(p))ALIGN((unsigned long long)(p), (a))

#define current Something that doesn't compile :)

#endif // LINUX_KERNEL_H_

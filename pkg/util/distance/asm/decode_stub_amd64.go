//go:build amd64

package asm

// HasF16C reports whether the CPU supports the F16C instruction (CPUID.1:ECX
// bit 20). golang.org/x/sys/cpu does not expose F16C, so it is probed here.
func HasF16C() bool

// DecodeFP16ToFloat32 decodes the little-endian float16 values in src into dst
// (dst must have room for len(src)/2 floats) using the F16C VCVTPH2PS
// instruction for 8 values at a time, scalar fallback for the tail.
func DecodeFP16ToFloat32(dst []float32, src []byte)

// DecodeInt8ToFloat32 decodes the signed int8 values in src into dst (dst must
// have room for len(src) floats) using AVX2 sign-extension, int8 signed, tail
// handled scalarly.
func DecodeInt8ToFloat32(dst []float32, src []byte)

// DecodeBFloat16ToFloat32 decodes the little-endian bfloat16 values in src into
// dst (dst must have room for len(src)/2 floats) using AVX2 shift-by-16, tail
// handled scalarly.
func DecodeBFloat16ToFloat32(dst []float32, src []byte)

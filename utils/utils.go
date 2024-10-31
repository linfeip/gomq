package utils

import "unsafe"

//go:linkname memmove runtime.memmove
//go:noescape
func memmove(dst, src unsafe.Pointer, size uintptr)

// Memmove memmove copies n bytes from "from" to "to".
func Memmove(dst, src unsafe.Pointer, size uintptr) {
	memmove(dst, src, size)
}

//go:linkname memequal runtime.memequal
//go:noescape
func memequal(a, b unsafe.Pointer, size uintptr) bool

// Memequal memory equal test
func Memequal(a, b unsafe.Pointer, size uintptr) bool {
	return memequal(a, b, size)
}

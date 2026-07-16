//go:build linux && arm64

package safety

import (
	"syscall"
	"unsafe"
)

const (
	lifecycleSYSNewfstatat = 79
	lifecycleSYSRenameat2  = 276
)

func lifecycleFstatat(directoryFD int, name string, stat *syscall.Stat_t, flags int) error {
	pointer, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(
		lifecycleSYSNewfstatat, uintptr(directoryFD), uintptr(unsafe.Pointer(pointer)),
		uintptr(unsafe.Pointer(stat)), uintptr(flags), 0, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func renameat2NoReplace(oldFD int, oldName string, newFD int, newName string) error {
	oldPointer, err := syscall.BytePtrFromString(oldName)
	if err != nil {
		return err
	}
	newPointer, err := syscall.BytePtrFromString(newName)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(
		lifecycleSYSRenameat2,
		uintptr(oldFD), uintptr(unsafe.Pointer(oldPointer)),
		uintptr(newFD), uintptr(unsafe.Pointer(newPointer)),
		renameNoReplace, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

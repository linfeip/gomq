package mmapfile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/linfeip/gomq/utils"
)

type MmapInfo struct {
	size     uint64
	readIdx  uint64
	writeIdx uint64
}

func NewMmapFile(filename string, size uint64) (*MmapFile, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return nil, err
	}

	m := &MmapFile{filename: filename, size: size}
	var err error
	if m.file, err = os.OpenFile(m.filename, os.O_RDWR|os.O_CREATE, 0666); nil != err {
		return nil, err
	}

	if err = m.file.Truncate(int64(m.size)); err != nil {
		m.file.Close()
		return nil, err
	}

	if m.mmap, err = mmap.Map(m.file, mmap.RDWR, 0); err != nil {
		m.file.Close()
		return nil, err
	}

	m.basep = unsafe.Pointer(unsafe.SliceData(m.mmap))
	return m, nil
}

// MmapFile 非线程安全的mmap映射对象
type MmapFile struct {
	filename string
	size     uint64
	file     *os.File
	mmap     mmap.MMap
	basep    unsafe.Pointer
	writeIdx uint64
	readIdx  uint64
	state    int32
}

func (m *MmapFile) Filename() string {
	return m.filename
}

func (m *MmapFile) Size() uint64 {
	return m.size
}

func (m *MmapFile) Read(p []byte) (n int, err error) {
	remain := m.writeIdx - m.readIdx
	if remain == 0 {
		return 0, io.EOF
	}
	ptr := unsafe.Pointer(uintptr(m.basep) + uintptr(m.readIdx))
	length := min(len(p), int(remain))
	utils.Memmove(unsafe.Pointer(unsafe.SliceData(p)), ptr, uintptr(length))
	m.readIdx += uint64(length)
	return length, nil
}

func (m *MmapFile) Write(p []byte) (n int, err error) {
	free := m.size - m.writeIdx
	if free < uint64(len(p)) {
		return 0, io.ErrShortWrite
	}
	writePtr := unsafe.Pointer(uintptr(m.basep) + uintptr(m.writeIdx))
	utils.Memmove(writePtr, unsafe.Pointer(unsafe.SliceData(p)), uintptr(len(p)))
	m.writeIdx += uint64(len(p))
	return len(p), nil
}

func (m *MmapFile) Close() error {
	return errors.Join(m.mmap.Unmap(), m.file.Close())
}

func (m *MmapFile) Flush() error {
	return m.mmap.Flush()
}

func (m *MmapFile) Base() unsafe.Pointer {
	return m.basep
}

func (m *MmapFile) Info() MmapInfo {
	return MmapInfo{size: m.size, readIdx: m.readIdx, writeIdx: m.writeIdx}
}

func (m *MmapFile) Free() uint64 {
	return m.size - m.writeIdx
}

func (m *MmapFile) ReadIndex() uint64 {
	return m.readIdx
}

func (m *MmapFile) WriteIndex() uint64 {
	return m.writeIdx
}

func (m *MmapFile) Reset(readIdx, writeIdx uint64) {
	m.readIdx, m.writeIdx = readIdx, writeIdx
}

func (m *MmapFile) StopWrite() {
	m.state = 1
}

func (m *MmapFile) IsStopWrite() bool {
	return m.state == 1
}

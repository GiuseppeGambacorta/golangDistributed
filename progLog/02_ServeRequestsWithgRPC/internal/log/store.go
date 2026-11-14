package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var encoding = binary.BigEndian

const SizeOfLenghtData = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(file *os.File) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	return &store{
		File: file,
		size: uint64(fileInfo.Size()),
		buf:  bufio.NewWriter(file),
	}, nil
}

func (store *store) Append(record []byte) (writtenBytes uint64, recordPosition uint64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	recordPosition = store.size
	lengthBytes := make([]byte, SizeOfLenghtData)
	encoding.PutUint64(lengthBytes, uint64(len(record)))
	if _, err := store.buf.Write(lengthBytes); err != nil {
		return 0, 0, err
	}
	bytesWritten, err := store.buf.Write(record)
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += SizeOfLenghtData
	store.size += uint64(bytesWritten)
	return uint64(bytesWritten), recordPosition, nil
}

func (store *store) Read(recordPosition uint64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if err := store.buf.Flush(); err != nil {
		return nil, err
	}
	lengthBytes := make([]byte, SizeOfLenghtData)
	if _, err := store.File.ReadAt(lengthBytes, int64(recordPosition)); err != nil {
		return nil, err
	}
	record := make([]byte, encoding.Uint64(lengthBytes))
	if _, err := store.File.ReadAt(record, int64(recordPosition+SizeOfLenghtData)); err != nil {
		return nil, err
	}
	return record, nil
}

func (store *store) ReadAt(buffer []byte, offset int64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.buf.Flush(); err != nil {
		return 0, err
	}
	return store.File.ReadAt(buffer, offset)
}

func (store *store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.buf.Flush(); err != nil {
		return err
	}
	return store.File.Close()
}

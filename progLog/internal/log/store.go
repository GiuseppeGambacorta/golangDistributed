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

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	return &store{
		File: f,
		size: uint64(fi.Size()),
		buf:  bufio.NewWriter(f),
	}, nil
}

// [lunghezza: 8 byte][contenuto: 4 byte]
// Il buffer accumula le scritture in memoria, così può scrivere nel file blocchi più grandi e ridurre le chiamate al sistema operativo.

func (s *store) Append(record []byte) (n uint64, posOfTheWritedRecord uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	posOfTheWritedRecord = s.size

	lengthBytes := make([]byte, SizeOfLenghtData)
	encoding.PutUint64(lengthBytes, uint64(len(record)))

	if _, err := s.buf.Write(lengthBytes); err != nil {
		return 0, 0, err
	}

	bytesWritten, err := s.buf.Write(record)
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += SizeOfLenghtData

	s.size += uint64(bytesWritten)
	return uint64(bytesWritten), posOfTheWritedRecord, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// readAt scrivere sul primo argomento
	// in lettura leggo sempre dal file direttamente
	recordLenght := make([]byte, SizeOfLenghtData)
	if _, err := s.File.ReadAt(recordLenght, int64(pos)); err != nil {
		return nil, err
	}
	recordValue := make([]byte, encoding.Uint64(recordLenght))
	if _, err := s.File.ReadAt(recordValue, int64(pos+SizeOfLenghtData)); err != nil {
		return nil, err
	}
	return recordValue, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}

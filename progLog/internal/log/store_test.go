package log

import (
	"bytes"
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + SizeOfLenghtData
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	t.Cleanup(func() {
		_ = f.Close()
		_ = os.Remove(name)
	})

	s, err := newStore(f)
	if err != nil {
		t.Fatal(err)
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	s, err = newStore(f)
	if err != nil {
		t.Fatal(err)
	}
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Fatal(err)
		}
		if want := pos + n; want != width*i {
			t.Fatalf("end position = %d, want %d", want, width*i)
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(write, read) {
			t.Fatalf("read = %q, want %q", read, write)
		}
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, SizeOfLenghtData)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != SizeOfLenghtData {
			t.Fatalf("length read = %d, want %d", n, SizeOfLenghtData)
		}
		off += int64(n)

		size := encoding.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(write, b) {
			t.Fatalf("read = %q, want %q", b, write)
		}
		if n != int(size) {
			t.Fatalf("length read = %d, want %d", n, size)
		}
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	t.Cleanup(func() {
		_ = os.Remove(name)
	})

	s, err := newStore(f)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Append(write); err != nil {
		t.Fatal(err)
	}

	beforeSize := fileSize(t, name)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	afterSize := fileSize(t, name)

	if afterSize <= beforeSize {
		t.Fatalf("file size after close = %d, before close = %d", afterSize, beforeSize)
	}
}

func fileSize(t *testing.T, name string) int64 {
	t.Helper()

	fi, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	byteOrder            = encoding
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file         *os.File
	mappedFile   gommap.MMap
	writtenBytes uint64
}

func newIndex(indexFile *os.File, config Config) (*index, error) {
	idx := &index{file: indexFile}
	fileInfo, err := os.Stat(indexFile.Name())
	if err != nil {
		return nil, err
	}
	idx.writtenBytes = uint64(fileInfo.Size())
	if err = os.Truncate(indexFile.Name(), int64(config.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mappedFile, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *index) Close() error {
	if err := idx.mappedFile.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.writtenBytes)); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *index) Read(requestedOffset int64) (recordOffset uint32, storePosition uint64, err error) {
	if idx.writtenBytes == 0 {
		return 0, 0, io.EOF
	}
	if requestedOffset == -1 {
		recordOffset = uint32((idx.writtenBytes / entryWidth) - 1)
	} else {
		recordOffset = uint32(requestedOffset)
	}
	entryStart := uint64(recordOffset) * entryWidth
	if idx.writtenBytes < entryStart+entryWidth {
		return 0, 0, io.EOF
	}
	recordOffset = byteOrder.Uint32(idx.mappedFile[entryStart : entryStart+offsetWidth])
	storePosition = byteOrder.Uint64(idx.mappedFile[entryStart+offsetWidth : entryStart+entryWidth])
	return recordOffset, storePosition, nil
}

func (idx *index) Write(recordOffset uint32, storePosition uint64) error {
	if uint64(len(idx.mappedFile)) < idx.writtenBytes+entryWidth {
		return io.EOF
	}
	byteOrder.PutUint32(idx.mappedFile[idx.writtenBytes:idx.writtenBytes+offsetWidth], recordOffset)
	byteOrder.PutUint64(idx.mappedFile[idx.writtenBytes+offsetWidth:idx.writtenBytes+entryWidth], storePosition)
	idx.writtenBytes += uint64(entryWidth)
	return nil
}

func (idx *index) Name() string {
	return idx.file.Name()
}

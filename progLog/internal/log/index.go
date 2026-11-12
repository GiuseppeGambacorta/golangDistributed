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

// newIndex prepara il file e lo espande alla dimensione massima configurata
// prima di mapparlo in memoria. Il file verra' ridotto alla dimensione reale
// durante Close.
func newIndex(indexFile *os.File, config Config) (*index, error) {
	idx := &index{
		file: indexFile,
	}
	fileInfo, err := os.Stat(indexFile.Name())
	if err != nil {
		return nil, err
	}
	idx.writtenBytes = uint64(fileInfo.Size())
	if err = os.Truncate(
		indexFile.Name(), int64(config.Segment.MaxIndexBytes),
	); err != nil {
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

// Close sincronizza la memoria mappata, salva il file e rimuove lo spazio
// preallocato che non contiene entry valide.
func (i *index) Close() error {
	if err := i.mappedFile.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.writtenBytes)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read restituisce l'offset e la posizione nello store dell'entry richiesta.
// Con requestedOffset == -1 viene letta l'ultima entry dell'indice.
func (i *index) Read(requestedOffset int64) (recordOffset uint32, storePosition uint64, err error) {
	if i.writtenBytes == 0 {
		return 0, 0, io.EOF
	}
	if requestedOffset == -1 {
		recordOffset = uint32((i.writtenBytes / entryWidth) - 1)
	} else {
		recordOffset = uint32(requestedOffset)
	}
	entryStart := uint64(recordOffset) * entryWidth
	if i.writtenBytes < entryStart+entryWidth {
		return 0, 0, io.EOF
	}
	recordOffset = byteOrder.Uint32(i.mappedFile[entryStart : entryStart+offsetWidth])
	storePosition = byteOrder.Uint64(i.mappedFile[entryStart+offsetWidth : entryStart+entryWidth])
	return recordOffset, storePosition, nil
}

// Write aggiunge una nuova entry alla fine dell'indice.
func (i *index) Write(recordOffset uint32, storePosition uint64) error {
	if uint64(len(i.mappedFile)) < i.writtenBytes+entryWidth {
		return io.EOF
	}
	byteOrder.PutUint32(i.mappedFile[i.writtenBytes:i.writtenBytes+offsetWidth], recordOffset)
	byteOrder.PutUint64(i.mappedFile[i.writtenBytes+offsetWidth:i.writtenBytes+entryWidth], storePosition)
	i.writtenBytes += uint64(entryWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

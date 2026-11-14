package log

import (
	"fmt"
	"os"
	"path"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"

	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	segment := &segment{baseOffset: baseOffset, config: config}
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if segment.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if segment.index, err = newIndex(indexFile, config); err != nil {
		return nil, err
	}
	if offset, _, err := segment.index.Read(-1); err != nil {
		segment.nextOffset = baseOffset
	} else {
		segment.nextOffset = baseOffset + uint64(offset) + 1
	}
	return segment, nil
}

func (segment *segment) Append(record *api.Record) (uint64, error) {
	currentOffset := segment.nextOffset
	record.Offset = currentOffset
	data, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, storePosition, err := segment.store.Append(data)
	if err != nil {
		return 0, err
	}
	if err := segment.index.Write(uint32(currentOffset-segment.baseOffset), storePosition); err != nil {
		return 0, err
	}
	segment.nextOffset++
	return currentOffset, nil
}

func (segment *segment) Read(offset uint64) (*api.Record, error) {
	_, storePosition, err := segment.index.Read(int64(offset - segment.baseOffset))
	if err != nil {
		return nil, err
	}
	data, err := segment.store.Read(storePosition)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (segment *segment) IsMaxed() bool {
	return segment.store.size >= segment.config.Segment.MaxStoreBytes ||
		segment.index.writtenBytes >= segment.config.Segment.MaxIndexBytes
}

func (segment *segment) Remove() error {
	if err := segment.Close(); err != nil {
		return err
	}
	if err := os.Remove(segment.index.Name()); err != nil {
		return err
	}
	return os.Remove(segment.store.Name())
}

func (segment *segment) Close() error {
	if err := segment.index.Close(); err != nil {
		return err
	}
	return segment.store.Close()
}

package log

import (
	"fmt"
	"os"
	"path"

	api "Distribuiti/progLog/01_StructureDataWithProtobuf/api/v1"

	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	segment := &segment{
		baseOffset: baseOffset,
		config:     config,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
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
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
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

func (segment *segment) Append(record *api.Record) (offset uint64, err error) {
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
	if err = segment.index.Write(
		uint32(segment.nextOffset-segment.baseOffset),
		storePosition,
	); err != nil {
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
	err = proto.Unmarshal(data, record)
	return record, err
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
	if err := os.Remove(segment.store.Name()); err != nil {
		return err
	}
	return nil
}

func (segment *segment) Close() error {
	if err := segment.index.Close(); err != nil {
		return err
	}
	if err := segment.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(value, multiple uint64) uint64 {
	if value >= 0 {
		return (value / multiple) * multiple
	}
	return ((value - multiple + 1) / multiple) * multiple
}

package main

import (
	"Distribuiti/progLog/StructureDataWithProtobuf/api/v1"
	"bytes"
	"errors"
	"sync"
)

type Log struct {
	mu      sync.RWMutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	record.Offset = uint64(len(l.records))
	record.Value = bytes.Clone(record.Value)

	l.records = append(l.records, record)

	return record.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	record := l.records[offset]
	record.Value = bytes.Clone(record.Value)

	return record, nil
}

type Record = logv1.Record

var ErrOffsetNotFound = errors.New("offset not found")

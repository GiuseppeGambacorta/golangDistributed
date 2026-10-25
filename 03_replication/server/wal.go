package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type WAL struct {
	file    *os.File
	mu      sync.Mutex
	walPath string
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		walPath: path,
	}, nil
}

func (w *WAL) WriteEntry(operation, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := fmt.Sprintf("%s %s %s\n", operation, key, value)
	_, err := w.file.WriteString(entry)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func ReplayWAL(path string, store *Store) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Split(line, " ")

		if len(parts) < 2 {
			continue
		}

		operation := parts[0]
		key := parts[1]

		switch operation {
		case "SET":
			value := ""
			if len(parts) >= 3 {
				value = strings.Join(parts[2:], " ")
			}
			store.mu.Lock()
			store.data[key] = value
			store.mu.Unlock()

		case "DELETE":
			store.mu.Lock()
			delete(store.data, key)
			store.mu.Unlock()
		}
	}

	return scanner.Err()
}

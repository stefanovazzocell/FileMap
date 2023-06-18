package filemap

import (
	"encoding/json"
	"os"
	"sync"
)

// A file-backed map with an in-memory index
type FileMap[K comparable, V any] struct {
	fileMapName string

	indexMu *sync.RWMutex
	index   map[K]int64

	fileMu *sync.Mutex
	file   *os.File
}

// Returns a new FM from an existing file
func OpenFileMap[K comparable, V any](fileMapName string) (*FileMap[K, V], error) {
	// Open FileMap
	file, err := os.Open(fileMapName) //#nosec
	if err != nil {
		return nil, err
	}
	// Load Header
	header, err := newHeaderFromReader(file)
	if err != nil {
		return nil, err
	}
	// Load Index
	if _, err := file.Seek(int64(header.indexOffset), 0); err != nil {
		return nil, err
	}
	// Decode Index
	marshaller := json.NewDecoder(file)
	index := map[K]int64{}
	if err := marshaller.Decode(&index); err != nil {
		return nil, err
	}
	// Return FileMap
	return &FileMap[K, V]{
		fileMapName: fileMapName,

		indexMu: &sync.RWMutex{},
		index:   index,

		fileMu: &sync.Mutex{},
		file:   file,
	}, nil
}

// Creates a new FM with a given set of data
func CreateFileMap[K comparable, V any](fileMapName string, data map[K]V) (*FileMap[K, V], error) {
	fm := FileMap[K, V]{
		fileMapName: fileMapName,

		indexMu: &sync.RWMutex{},
		index:   nil,

		fileMu: &sync.Mutex{},
		file:   nil,
	}
	err := fm.Update(data)
	return &fm, err
}

// Close file and reset index
func (fm *FileMap[K, V]) Close() error {
	fm.indexMu.Lock()
	fm.fileMu.Lock()
	defer fm.indexMu.Unlock()
	defer fm.fileMu.Unlock()
	fm.index = nil
	if err := fm.file.Close(); err != nil {
		return err
	}
	return fm.cleanupTemp()
}

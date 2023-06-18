package filemap

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

var (
	// Error returned when the data to insert into a FM is nil
	ErrDataIsNil = errors.New("data is nil")
)

// Returns a temporary fm file to use
func (fm *FileMap[K, V]) tmpFileName() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fm.fileMapName + "." + strconv.FormatUint(binary.BigEndian.Uint64(b), 10) + ".tmp", nil
}

// Updates a FM with the data provided
//
// This attempts to acquire the locks for the least amount of time possible
func (fm *FileMap[K, V]) Update(data map[K]V) error {
	if data == nil {
		return ErrDataIsNil
	}
	// Prepare header
	header := newHeader()
	// Open temporary file
	var tmpFileName string
	var err error
	var tmpFile *os.File
	for {
		// Keep trying until a free temporary file is found
		tmpFileName, err = fm.tmpFileName()
		if err != nil {
			return err
		}
		tmpFile, err = os.OpenFile(tmpFileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_EXCL, 0600) //#nosec
		if err == nil {
			break //
		}
		if err != os.ErrExist {
			return err
		}
	}
	n, err := tmpFile.Write(header.Bytes())
	if err != nil {
		_ = tmpFile.Close()
		return err
	}
	dataLocation := int64(n)
	fileLocation := n
	// Write data and build index
	index := make(map[K]int64, len(data))
	for key, entry := range data {
		index[key] = int64(fileLocation)
		entryBytes, err := json.Marshal(entry)
		if err != nil {
			_ = tmpFile.Close()
			return err
		}
		n, err := tmpFile.Write(entryBytes)
		if err != nil {
			_ = tmpFile.Close()
			return err
		}
		fileLocation += n
	}
	// Write index
	indexBytes, err := json.Marshal(index)
	if err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Write(indexBytes); err != nil {
		_ = tmpFile.Close()
		return err
	}
	// Update header
	header.Update(dataLocation, int64(fileLocation))
	if _, err := tmpFile.Seek(0, 0); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Write(header.Bytes()); err != nil {
		_ = tmpFile.Close()
		return err
	}
	// Close tmp file
	if err := tmpFile.Close(); err != nil {
		return err
	}
	// Acquire locks
	fm.indexMu.Lock()
	fm.fileMu.Lock()
	// Swap the index
	fm.index = index
	// Swap the fm
	_ = fm.file.Close()
	if err := os.Rename(tmpFileName, fm.fileMapName); err != nil {
		fm.index = nil
		fm.file = nil
		fm.indexMu.Unlock()
		fm.fileMu.Unlock()
		return err
	}
	fm.file, err = os.Open(fm.fileMapName) //#nosec
	if err != nil {
		fm.index = nil
		fm.file = nil
		fm.indexMu.Unlock()
		fm.fileMu.Unlock()
		return err
	}
	// Release locks
	fm.indexMu.Unlock()
	fm.fileMu.Unlock()
	// Call garbage collector
	runtime.GC()
	return nil
}

// Cleans up temporary files created by the updater
func (fm *FileMap[K, V]) cleanupTemp() error {
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(fm.fileMapName), fmt.Sprintf("%s.*.tmp", filepath.Base(fm.fileMapName))))
	if err != nil {
		return err
	}
	// Remove all matches, ignore errors
	for _, match := range matches {
		_ = os.Remove(match)
	}
	return nil
}

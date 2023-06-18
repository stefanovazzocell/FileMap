package filemap_test

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	filemap "github.com/stefanovazzocell/FileMap"
)

type testDataStruct struct {
	Id   string
	Blob []byte
}

var (
	testData = map[string]testDataStruct{
		"0": {Id: "0", Blob: []byte{0, 1, 2, 3, 4, 5}},
		"1": {Id: "1", Blob: []byte{10, 11, 12, 13, 14, 15}},
		"2": {Id: "2", Blob: []byte{20, 21, 22, 23, 24, 25}},
		"3": {Id: "3", Blob: []byte{30, 31, 32, 33, 34, 35}},
		"4": {Id: "4", Blob: []byte{40, 41, 42, 43, 44, 45}},
		"5": {Id: "5", Blob: []byte{50, 51, 52, 53, 54, 55}},
	}
)

// Helper to cleanup test
func cleanupFile(fileName string) {
	os.Remove(fileName)
}

// Helper to test fm
func fmTestHelper(database *filemap.FileMap[string, testDataStruct], t *testing.T) {
	// Open/Read FileMap file
	file, err := os.Open("./testfm")
	if err != nil {
		t.Fatalf("Failed to open FileMap file: %v", err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read FileMap file: %v", err)
	}
	if len(data) < 40*5 {
		t.Fatalf("Database data is smaller than expected: %q", data)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close FileMap file: %v", err)
	}
	// Query FileMap - Existing entry
	entry, err := database.Lookup("4")
	if err != nil {
		t.Fatalf("Failed to lookup existing entry: %v", err)
	}
	if entry != nil && !reflect.DeepEqual(*entry, testData["4"]) {
		t.Fatalf("Expected entry %v, instead got: %v", testData["4"], entry)
	}
	// Query FileMap - Missing entry
	entry, err = database.Lookup("10")
	if err != filemap.ErrNotFound {
		t.Fatalf("Unexpected error for lookup of missing entry: %v", err)
	}
	if entry != nil {
		t.Fatalf("Expected empty entry, instead got: %v", entry)
	}
	// Query FileMap - Existing entries
	entries, err := database.LookupMany([]string{"2", "10", "1"})
	if err != nil {
		t.Fatalf("Failed to lookup existing entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, instead got: %v", entries)
	}
	if !reflect.DeepEqual(entries["1"], testData["1"]) || !reflect.DeepEqual(entries["2"], testData["2"]) {
		t.Fatalf("Got unexpected entries: %v", entries)
	}
	// Query FileMap - Missing entries
	entries, err = database.LookupMany([]string{"11", "12", "10"})
	if err != nil {
		t.Fatalf("Failed to lookup missing entries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Expected no entries, instead got: %v", entries)
	}
	// Close FileMap
	if err := database.Close(); err != nil {
		t.Fatalf("Failed to close FileMap properly: %v", err)
	}
}

func TestFileMap(t *testing.T) {
	// Init
	t.Parallel()
	cleanupFile("./testfm")
	defer cleanupFile("./testfm")
	// Create FileMap
	database, err := filemap.CreateFileMap("./testfm", testData)
	if err != nil {
		t.Fatalf("Failed to create FileMap: %v", err)
	}
	t.Log("Testing created FileMap...")
	fmTestHelper(database, t)
	// Open FileMap
	database, err = filemap.OpenFileMap[string, testDataStruct]("./testfm")
	if err != nil {
		t.Fatalf("Failed to open FileMap: %v", err)
	}
	t.Log("Testing opened FileMap...")
	fmTestHelper(database, t)
}

func TestParallelCalls(t *testing.T) {
	// Init
	t.Parallel()
	cleanupFile("./testfmParallel")
	defer cleanupFile("./testfmParallel")
	// Create FileMap
	database, err := filemap.CreateFileMap("./testfmParallel", testData)
	if err != nil {
		t.Fatalf("Failed to create FileMap: %v", err)
	}
	// Start concurrent reads and updates
	workers := runtime.NumCPU()
	errChan := make(chan error, workers*3)
	lookups := 10000
	lookupManys := 1000
	updates := 100
	for w := 0; w < workers; w++ {
		// Lookup
		go func() {
			for i := 0; i < lookups; i++ {
				// Query FileMap - Existing entry
				entry, err := database.Lookup("4")
				if err != nil {
					errChan <- fmt.Errorf("Failed to lookup existing entry: %v", err)
					return
				}
				if entry != nil && !reflect.DeepEqual(*entry, testData["4"]) {
					errChan <- fmt.Errorf("Expected entry %v, instead got: %v", testData["4"], entry)
					return
				}
				// Query FileMap - Missing entry
				entry, err = database.Lookup("10")
				if err != filemap.ErrNotFound {
					errChan <- fmt.Errorf("Unexpected error for lookup of missing entry: %v", err)
					return
				}
				if entry != nil {
					errChan <- fmt.Errorf("Expected empty entry, instead got: %v", entry)
					return
				}
			}
			errChan <- nil
		}()
		// LookupMany
		go func() {
			for i := 0; i < lookupManys; i++ {
				// Query FileMap - Existing entries
				entries, err := database.LookupMany([]string{"2", "10", "1"})
				if err != nil {
					errChan <- fmt.Errorf("Failed to lookup existing entries: %v", err)
					return
				}
				if len(entries) != 2 {
					errChan <- fmt.Errorf("Expected 2 entries, instead got: %v", entries)
					return
				}
				if !reflect.DeepEqual(entries["1"], testData["1"]) || !reflect.DeepEqual(entries["2"], testData["2"]) {
					errChan <- fmt.Errorf("Got unexpected entries: %v", entries)
					return
				}
				// Query FileMap - Missing entries
				entries, err = database.LookupMany([]string{"11", "12", "10"})
				if err != nil {
					errChan <- fmt.Errorf("Failed to lookup missing entries: %v", err)
					return
				}
				if len(entries) != 0 {
					errChan <- fmt.Errorf("Expected no entries, instead got: %v", entries)
					return
				}
			}
			errChan <- nil
		}()
		// Updates
		go func() {
			for i := 0; i < updates; i++ {
				err := database.Update(testData)
				if err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}()
	}
	// Await for completion
	for i := 0; i < workers*3; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Got error %v", err)
		}
	}
	// Close
	if err := database.Close(); err != nil {
		t.Errorf("Failed to close fm %v", err)
	}
}

// Helper to create test data
func createData(length int, size int) map[int]testDataStruct {
	data := make(map[int]testDataStruct, length)
	for i := 0; i < length; i++ {
		data[i] = testDataStruct{Id: "", Blob: make([]byte, size)}
	}
	return data
}

// Helper to create FileMaps
func createFileMap(data map[int]testDataStruct, b *testing.B) {
	rb := make([]byte, 8)
	if _, err := rand.Read(rb); err != nil {
		panic(err)
	}
	fmName := "./testfm_bench." + strconv.FormatUint(binary.BigEndian.Uint64(rb), 10) + ""
	cleanupFile(fmName)
	b.StartTimer()
	database, err := filemap.CreateFileMap(fmName, testData)
	b.StopTimer()
	defer cleanupFile(fmName)
	if err != nil {
		b.Fatalf("Error creating FileMap: %v", err)
	}
	if err := database.Close(); err != nil {
		b.Fatalf("Error closing FileMap: %v", err)
	}
}

func BenchmarkDatabase1M(b *testing.B) {
	// Setup
	testData := createData(1000000, 1024)
	cleanupFile("./benchfm")
	defer cleanupFile("./benchfm")
	database, err := filemap.CreateFileMap("./benchfm", testData)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()
	// Test FileMap Create
	b.Run("Create", func(b *testing.B) {
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			createFileMap(testData, b)
		}
	})
	// Test Lookup
	b.Run("Lookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := database.Lookup(i); err != nil && err != filemap.ErrNotFound {
				b.Fatalf("Got error %v", err)
			}
		}
	})
	b.Run("LookupParallel", func(b *testing.B) {
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				if _, err := database.Lookup(10); err != nil && err != filemap.ErrNotFound {
					b.Fatalf("Got error %v", err)
				}
			}
		})
	})
	// Test LookupMany
	b.Run("LookupMany5", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := database.LookupMany([]int{i, i + 1, i + 2, i + 3, i + 4}); err != nil && err != filemap.ErrNotFound {
				b.Fatalf("Got error %v", err)
			}
		}
	})
	b.Run("LookupMany5Parallel", func(b *testing.B) {
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				if _, err := database.LookupMany([]int{0, 1, 2, 3, 4}); err != nil && err != filemap.ErrNotFound {
					b.Fatalf("Got error %v", err)
				}
			}
		})
	})
}

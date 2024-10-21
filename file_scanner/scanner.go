package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	totalFilesScanned int64
	totalFilesWritten int64
	scanStartTime     time.Time
	lastUpdateTime    time.Time
	lastFilesScanned  int64
	lastFilesWritten  int64
)

const (
	batchSize  = 100
	numWorkers = 10
)

func scanFolder(ctx context.Context, db *sql.DB, tableName, folderPath string) error {
	fileChan := make(chan string, 10000)
	resultChan := make(chan FileInfo, 10000)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Reset counters
	atomic.StoreInt64(&totalFilesScanned, 0)
	atomic.StoreInt64(&totalFilesWritten, 0)
	scanStartTime = time.Now()
	lastUpdateTime = scanStartTime
	lastFilesScanned = 0
	lastFilesWritten = 0

	log.Printf("Starting scan of folder: %s", folderPath)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range fileChan {
				if paused {
					for paused {
						time.Sleep(500 * time.Millisecond)
						if ctx.Err() != nil {
							return
						}
					}
				}
				select {
				case <-ctx.Done():
					return
				default:
					fileInfo, err := processFile(filePath)
					if err != nil {
						log.Printf("Error processing file %s: %v", filePath, err)
						continue // Skip this file and continue with others
					}
					resultChan <- fileInfo
					atomic.AddInt64(&totalFilesScanned, 1)
				}
			}
		}()
	}

	// Start batch insert worker
	go func() {
		defer close(resultChan)
		batch := make([]FileInfo, 0, batchSize)
		for fileInfo := range resultChan {
			batch = append(batch, fileInfo)
			if len(batch) >= batchSize {
				if err := batchInsert(db, tableName, batch); err != nil {
					log.Printf("Error batch inserting: %v", err)
					errChan <- fmt.Errorf("error batch inserting: %v", err)
					return
				}
				atomic.AddInt64(&totalFilesWritten, int64(len(batch)))
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			if err := batchInsert(db, tableName, batch); err != nil {
				log.Printf("Error batch inserting final batch: %v", err)
				errChan <- fmt.Errorf("error batch inserting final batch: %v", err)
				return
			}
			atomic.AddInt64(&totalFilesWritten, int64(len(batch)))
		}
	}()

	// Walk the folder and send files to fileChan
	go func() {
		defer close(fileChan)
		err := filepath.WalkDir(folderPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				log.Printf("Error walking directory at %s: %v", path, err)
				return nil // Continue walking despite the error
			}
			if d.IsDir() {
				return nil
			}
			scanStateLock.Lock()
			if scanState.FilesScanned[path] {
				scanStateLock.Unlock()
				return nil
			}
			scanState.FilesScanned[path] = true
			scanStateLock.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case fileChan <- path:
				return nil
			}
		})
		if err != nil {
			log.Printf("Error walking directory: %v", err)
			errChan <- fmt.Errorf("error walking directory: %v", err)
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	select {
	case <-ctx.Done():
		log.Println("Scan cancelled")
		return ctx.Err()
	case err := <-errChan:
		log.Printf("Scan completed with error: %v", err)
		return err
	case <-resultChan:
		log.Println("Scan completed successfully")
	}

	return nil
}

func processFile(filePath string) (FileInfo, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return FileInfo{}, fmt.Errorf("error getting file info: %v", err)
	}

	// Create a unique hash based on the file path
	hasher := sha256.New()
	hasher.Write([]byte(filePath))
	pathHash := hex.EncodeToString(hasher.Sum(nil))

	return FileInfo{
		FileName:      info.Name(),
		FilePath:      filePath,
		PathHash:      pathHash,
		FileSize:      info.Size(),
		ModTime:       info.ModTime(),
		OtherMetadata: "", // You may want to implement other metadata collection
		Extension:     filepath.Ext(filePath),
	}, nil
}

func GetProgressStats() (int64, int64, float64, float64) {
	now := time.Now()
	updateDuration := now.Sub(lastUpdateTime)

	filesScanned := atomic.LoadInt64(&totalFilesScanned)
	filesWritten := atomic.LoadInt64(&totalFilesWritten)

	scanSpeed := float64(filesScanned-lastFilesScanned) / updateDuration.Seconds()
	writeSpeed := float64(filesWritten-lastFilesWritten) / updateDuration.Seconds()

	lastUpdateTime = now
	lastFilesScanned = filesScanned
	lastFilesWritten = filesWritten

	return filesScanned, filesWritten, scanSpeed, writeSpeed
}

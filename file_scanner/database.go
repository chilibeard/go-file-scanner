package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

type FileInfo struct {
	FileName      string
	FilePath      string
	PathHash      string
	FileSize      int64
	ModTime       time.Time
	OtherMetadata string
	Extension     string
}

func createTable(db *sql.DB, tableName string) error {
	query := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%s' and xtype='U')
	CREATE TABLE %s (
		Id INT PRIMARY KEY IDENTITY(1,1),
		file_name NVARCHAR(255) NOT NULL,
		file_path NVARCHAR(MAX) NULL,
		path_hash VARCHAR(64) NOT NULL UNIQUE,
		file_size BIGINT NOT NULL,
		mod_time DATETIME2(7) NOT NULL,
		other_metadata NVARCHAR(MAX) NULL,
		extension NVARCHAR(50) NULL
	)`, tableName, tableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	log.Printf("Table '%s' created or already exists", tableName)
	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	query := `SELECT TABLE_NAME 
			  FROM INFORMATION_SCHEMA.TABLES 
			  WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = DB_NAME()`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func batchInsert(db *sql.DB, tableName string, files []FileInfo) error {
	if len(files) == 0 {
		return nil
	}

	// Prepare the MERGE statement
	query := fmt.Sprintf(`
	MERGE INTO %s AS target
	USING (VALUES `, tableName)

	// Prepare the values and parameters
	valueStrings := make([]string, 0, len(files))
	valueArgs := make([]interface{}, 0, len(files)*7)
	for i, file := range files {
		valueStrings = append(valueStrings, fmt.Sprintf("(@p%d, @p%d, @p%d, @p%d, @p%d, @p%d, @p%d)",
			i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+1), file.FileName))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+2), file.FilePath))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+3), file.PathHash))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+4), file.FileSize))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+5), file.ModTime))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+6), file.OtherMetadata))
		valueArgs = append(valueArgs, sql.Named(fmt.Sprintf("p%d", i*7+7), file.Extension))
	}

	// Complete the query
	query += strings.Join(valueStrings, ",")
	query += `) AS source (file_name, file_path, path_hash, file_size, mod_time, other_metadata, extension)
	ON target.path_hash = source.path_hash
	WHEN MATCHED THEN
		UPDATE SET
			file_name = source.file_name,
			file_path = source.file_path,
			file_size = source.file_size,
			mod_time = source.mod_time,
			other_metadata = source.other_metadata,
			extension = source.extension
	WHEN NOT MATCHED THEN
		INSERT (file_name, file_path, path_hash, file_size, mod_time, other_metadata, extension)
		VALUES (source.file_name, source.file_path, source.path_hash, source.file_size, source.mod_time, source.other_metadata, source.extension);`

	log.Printf("Executing batch merge for %d files", len(files))
	log.Printf("Query: %s", query)

	// Execute the query
	_, err := db.Exec(query, valueArgs...)
	if err != nil {
		log.Printf("Error executing batch merge query: %v", err)
		return fmt.Errorf("error batch merging: %v", err)
	}

	log.Printf("Successfully merged %d files into the database", len(files))
	return nil
}

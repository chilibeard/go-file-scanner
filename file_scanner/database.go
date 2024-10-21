package main

import (
	"database/sql"
	"fmt"
	"time"
)

type FileInfo struct {
	Name         string
	Size         int64
	LastModified time.Time
	ETag         string
	PathHash     string
	ItemPath     string
	Extension    string
}

func createTable(db *sql.DB, tableName string) error {
	query := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%s' and xtype='U')
	CREATE TABLE %s (
		Id INT IDENTITY(1,1) PRIMARY KEY,
		Name NVARCHAR(255) NOT NULL,
		Size BIGINT NOT NULL,
		LastModified DATETIME2(7) NOT NULL,
		ETag NVARCHAR(100) NULL,
		PathHash CHAR(64) NOT NULL,
		ItemPath NVARCHAR(1000) NOT NULL,
		Extension NVARCHAR(50) NULL
	)`, tableName, tableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}
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

	query := fmt.Sprintf(`
	INSERT INTO %s (Name, Size, LastModified, ETag, PathHash, ItemPath, Extension)
	VALUES `, tableName)

	vals := []interface{}{}
	for i, file := range files {
		if i > 0 {
			query += ","
		}
		query += "(?, ?, ?, ?, ?, ?, ?)"
		vals = append(vals, file.Name, file.Size, file.LastModified, file.ETag, file.PathHash, file.ItemPath, file.Extension)
	}

	query += `
	ON DUPLICATE KEY UPDATE
	Name = VALUES(Name),
	Size = VALUES(Size),
	LastModified = VALUES(LastModified),
	ETag = VALUES(ETag),
	ItemPath = VALUES(ItemPath),
	Extension = VALUES(Extension)`

	_, err := db.Exec(query, vals...)
	if err != nil {
		return fmt.Errorf("error batch inserting: %v", err)
	}

	return nil
}

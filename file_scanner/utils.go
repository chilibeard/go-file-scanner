package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	"fyne.io/fyne/v2/widget"
)

func getAppDataDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	appDataDir := filepath.Join(homeDir, ".file_scanner")
	err = os.MkdirAll(appDataDir, 0755)
	if err != nil {
		return "", err
	}
	return appDataDir, nil
}

func getScanStatePath() (string, error) {
	appDataDir, err := getAppDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(appDataDir, "scan_state.gob"), nil
}

// Functions to save and load scan state
func saveScanState() error {
	scanStatePath, err := getScanStatePath()
	if err != nil {
		return fmt.Errorf("error getting scan state path: %v", err)
	}

	file, err := os.Create(scanStatePath)
	if err != nil {
		return fmt.Errorf("error creating scan state file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(scanState)
	if err != nil {
		return fmt.Errorf("error encoding scan state: %v", err)
	}
	return nil
}

func loadScanState() error {
	scanStatePath, err := getScanStatePath()
	if err != nil {
		return fmt.Errorf("error getting scan state path: %v", err)
	}

	file, err := os.Open(scanStatePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // It's okay if the file doesn't exist
		}
		return fmt.Errorf("error opening scan state file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&scanState)
	if err != nil {
		return fmt.Errorf("error decoding scan state: %v", err)
	}
	return nil
}

func scanStateExists() (bool, error) {
	scanStatePath, err := getScanStatePath()
	if err != nil {
		return false, fmt.Errorf("error getting scan state path: %v", err)
	}
	_, err = os.Stat(scanStatePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func deleteScanState() error {
	scanStatePath, err := getScanStatePath()
	if err != nil {
		return fmt.Errorf("error getting scan state path: %v", err)
	}
	err = os.Remove(scanStatePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting scan state file: %v", err)
	}
	return nil
}

// Functions to save and load credentials
func saveCredentials(ipEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry *widget.Entry) error {
	appDataDir, err := getAppDataDir()
	if err != nil {
		return fmt.Errorf("error getting app data directory: %v", err)
	}
	credentialsPath := filepath.Join(appDataDir, "credentials.gob")

	file, err := os.Create(credentialsPath)
	if err != nil {
		return fmt.Errorf("error creating credentials file: %v", err)
	}
	defer file.Close()

	credentials := map[string]string{
		"ip":       ipEntry.Text,
		"port":     portEntry.Text,
		"username": usernameEntry.Text,
		"password": passwordEntry.Text,
		"dbname":   dbNameEntry.Text,
	}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(credentials)
	if err != nil {
		return fmt.Errorf("error encoding credentials: %v", err)
	}
	return nil
}

func loadCredentials(ipEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry *widget.Entry) error {
	appDataDir, err := getAppDataDir()
	if err != nil {
		return fmt.Errorf("error getting app data directory: %v", err)
	}
	credentialsPath := filepath.Join(appDataDir, "credentials.gob")

	file, err := os.Open(credentialsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // It's okay if the file doesn't exist
		}
		return fmt.Errorf("error opening credentials file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	credentials := make(map[string]string)
	err = decoder.Decode(&credentials)
	if err != nil {
		return fmt.Errorf("error decoding credentials: %v", err)
	}

	ipEntry.SetText(credentials["ip"])
	portEntry.SetText(credentials["port"])
	usernameEntry.SetText(credentials["username"])
	passwordEntry.SetText(credentials["password"])
	dbNameEntry.SetText(credentials["dbname"])
	return nil
}

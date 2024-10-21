package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	_ "github.com/denisenkom/go-mssqldb"
)

type ScanState struct {
	FolderPath   string
	FilesScanned map[string]bool
	LastModified time.Time
}

var (
	scanState     ScanState
	scanStateLock sync.Mutex
	scanning      bool
	paused        bool
	cancelFunc    context.CancelFunc
	scanDone      chan struct{}
	logChan       chan string
)

func main() {
	// Set up logging
	logFile, err := os.OpenFile("file_scanner.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Set up panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recovered: %v\n%s", r, debug.Stack())
			fmt.Printf("The application has crashed. Please check the log file 'file_scanner.log' for details.\n")
		}
	}()

	logChan = make(chan string, 100)

	myApp := app.New()
	myWindow := myApp.NewWindow("File Scanner")

	// Database connection fields
	serverEntry := widget.NewEntry()
	serverEntry.SetPlaceHolder("Server")

	portEntry := widget.NewEntry()
	portEntry.SetPlaceHolder("Port (default 1433)")

	usernameEntry := widget.NewEntry()
	usernameEntry.SetPlaceHolder("Username")

	passwordEntry := widget.NewPasswordEntry()
	passwordEntry.SetPlaceHolder("Password")

	dbNameEntry := widget.NewEntry()
	dbNameEntry.SetPlaceHolder("Database Name")

	// Load saved credentials if available
	if err := loadCredentials(serverEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry); err != nil {
		log.Printf("Error loading credentials: %v", err)
		dialog.ShowError(err, myWindow)
	}

	// Status label
	statusLabel := widget.NewLabel("Status: Not connected")

	// Progress label
	progressLabel := widget.NewLabel("Progress: Not started")

	// Log viewer
	logViewer := widget.NewMultiLineEntry()
	logViewer.Disable()

	// Connect button
	connectButton := widget.NewButton("Connect", nil)

	// Create or select table buttons
	createTableButton := widget.NewButton("Create New Table", nil)
	selectTableButton := widget.NewButton("Select Existing Table", nil)

	// Folder selection
	folderEntry := widget.NewEntry()
	folderEntry.SetPlaceHolder("Enter or select folder path to scan")
	browseButton := widget.NewButton("Browse", func() {
		dialog.ShowFolderOpen(func(uri fyne.ListableURI, err error) {
			if err != nil {
				log.Printf("Error opening folder dialog: %v", err)
				dialog.ShowError(err, myWindow)
				return
			}
			if uri != nil {
				folderEntry.SetText(uri.Path())
			}
		}, myWindow)
	})

	// Manual path input button
	manualPathButton := widget.NewButton("Enter UNC Path", func() {
		entry := widget.NewEntry()
		entry.SetPlaceHolder("Enter UNC path (e.g., \\\\server\\share)")
		dialog.ShowCustomConfirm("Enter UNC Path", "OK", "Cancel", entry, func(b bool) {
			if b {
				folderEntry.SetText(entry.Text)
			}
		}, myWindow)
	})

	// Scan control buttons
	startButton := widget.NewButton("Start Scan", nil)
	pauseButton := widget.NewButton("Pause Scan", nil)
	resumeButton := widget.NewButton("Resume Scan", nil)
	stopButton := widget.NewButton("Stop Scan", nil)

	// Disable buttons initially
	createTableButton.Disable()
	selectTableButton.Disable()
	startButton.Disable()
	pauseButton.Disable()
	resumeButton.Disable()
	stopButton.Disable()

	// Database connection
	var db *sql.DB
	connectButton.OnTapped = func() {
		server := serverEntry.Text
		port := portEntry.Text
		if port == "" {
			port = "1433" // Default SQL Server port
		}
		username := usernameEntry.Text
		password := passwordEntry.Text
		dbName := dbNameEntry.Text

		// Connection string for SQL Server
		connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
			server, username, password, port, dbName)

		// Open database connection
		var err error
		db, err = sql.Open("sqlserver", connString)
		if err != nil {
			log.Printf("Error opening database connection: %v", err)
			statusLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}

		// Test the connection
		err = db.Ping()
		if err != nil {
			log.Printf("Error pinging database: %v", err)
			statusLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}

		// Save credentials
		if err := saveCredentials(serverEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry); err != nil {
			log.Printf("Error saving credentials: %v", err)
			dialog.ShowError(err, myWindow)
		}

		log.Println("Database connected successfully")
		statusLabel.SetText("Status: Connected successfully")
		createTableButton.Enable()
		selectTableButton.Enable()
	}

	// Table selection
	var tableName string
	createTableButton.OnTapped = func() {
		entry := widget.NewEntry()
		entry.SetPlaceHolder("Enter New Table Name")
		dialog.ShowCustomConfirm("Create New Table", "Create", "Cancel", entry, func(b bool) {
			if b {
				tableName = entry.Text
				// Create table
				err := createTable(db, tableName)
				if err != nil {
					log.Printf("Error creating table: %v", err)
					statusLabel.SetText(fmt.Sprintf("Error creating table: %v", err))
					return
				}
				log.Printf("Table '%s' created successfully", tableName)
				statusLabel.SetText(fmt.Sprintf("Table '%s' created successfully", tableName))
				startButton.Enable()
			}
		}, myWindow)
	}

	selectTableButton.OnTapped = func() {
		// Retrieve existing tables
		tables, err := getTables(db)
		if err != nil {
			log.Printf("Error getting tables: %v", err)
			statusLabel.SetText(fmt.Sprintf("Error getting tables: %v", err))
			return
		}
		if len(tables) == 0 {
			log.Println("No existing tables found")
			statusLabel.SetText("No existing tables found")
			return
		}

		// Let user select a table
		tableSelect := widget.NewSelect(tables, func(value string) {
			tableName = value
		})
		dialog.ShowCustomConfirm("Select Table", "Select", "Cancel", tableSelect, func(b bool) {
			if b && tableName != "" {
				log.Printf("Table '%s' selected", tableName)
				statusLabel.SetText(fmt.Sprintf("Table '%s' selected", tableName))
				startButton.Enable()
			}
		}, myWindow)
	}

	// Scan control functions
	startButton.OnTapped = func() {
		if folderEntry.Text == "" {
			log.Println("Error: No folder path provided")
			statusLabel.SetText("Error: Please enter or select a folder path to scan")
			return
		}

		scanning = true
		paused = false
		startButton.Disable()
		pauseButton.Enable()
		stopButton.Enable()
		statusLabel.SetText("Status: Scanning")

		// Load previous scan state if available
		if err := loadScanState(); err != nil {
			log.Printf("Error loading scan state: %v", err)
			dialog.ShowError(fmt.Errorf("Error loading scan state: %v", err), myWindow)
		}

		scanState.FolderPath = folderEntry.Text
		if scanState.FilesScanned == nil {
			scanState.FilesScanned = make(map[string]bool)
		}

		var ctx context.Context
		ctx, cancelFunc = context.WithCancel(context.Background())
		scanDone = make(chan struct{})

		go func() {
			defer close(scanDone)
			err := scanFolder(ctx, db, tableName, scanState.FolderPath)
			if err != nil {
				if err == context.Canceled {
					log.Println("Scan stopped")
					statusLabel.SetText("Status: Scan stopped")
				} else {
					log.Printf("Error during scan: %v", err)
					statusLabel.SetText(fmt.Sprintf("Error during scan: %v", err))
				}
			} else {
				log.Println("Scan completed successfully")
				statusLabel.SetText("Status: Scan completed successfully")
			}
			scanStateLock.Lock()
			if err := saveScanState(); err != nil {
				log.Printf("Error saving scan state: %v", err)
				dialog.ShowError(fmt.Errorf("Error saving scan state: %v", err), myWindow)
			}
			scanStateLock.Unlock()
			scanning = false
			startButton.Enable()
			pauseButton.Disable()
			resumeButton.Disable()
			stopButton.Disable()
			if err := deleteScanState(); err != nil {
				log.Printf("Error deleting scan state: %v", err)
				dialog.ShowError(fmt.Errorf("Error deleting scan state: %v", err), myWindow)
			}
		}()

		// Start progress update routine
		go func() {
			for scanning {
				time.Sleep(1 * time.Second)
				filesScanned, filesWritten, scanSpeed, writeSpeed := GetProgressStats()
				progressText := fmt.Sprintf("Progress: Scanned %d files, Written %d files\nScan speed: %.2f files/sec, Write speed: %.2f files/sec",
					filesScanned, filesWritten, scanSpeed, writeSpeed)
				log.Println(progressText)
				progressLabel.SetText(progressText)
			}
		}()
	}

	pauseButton.OnTapped = func() {
		paused = true
		pauseButton.Disable()
		resumeButton.Enable()
		log.Println("Scan paused")
		statusLabel.SetText("Status: Paused")
	}

	resumeButton.OnTapped = func() {
		paused = false
		pauseButton.Enable()
		resumeButton.Disable()
		log.Println("Scan resumed")
		statusLabel.SetText("Status: Scanning")
	}

	stopButton.OnTapped = func() {
		if cancelFunc != nil {
			cancelFunc()
		}
		log.Println("Stopping scan...")
		statusLabel.SetText("Status: Stopping scan...")
		go func() {
			<-scanDone
			scanning = false
			paused = false
			startButton.Enable()
			pauseButton.Disable()
			resumeButton.Disable()
			stopButton.Disable()
			log.Println("Scan stopped")
			statusLabel.SetText("Status: Scan stopped")
		}()
	}

	// Check for existing scan state
	exists, err := scanStateExists()
	if err != nil {
		log.Printf("Error checking scan state: %v", err)
		dialog.ShowError(fmt.Errorf("Error checking scan state: %v", err), myWindow)
	} else if exists {
		dialog.ShowConfirm("Resume Scan", "A previous scan was not completed. Do you want to resume?", func(b bool) {
			if b {
				if err := loadScanState(); err != nil {
					log.Printf("Error loading scan state: %v", err)
					dialog.ShowError(fmt.Errorf("Error loading scan state: %v", err), myWindow)
				} else {
					folderEntry.SetText(scanState.FolderPath)
					startButton.Enable()
				}
			} else {
				if err := deleteScanState(); err != nil {
					log.Printf("Error deleting scan state: %v", err)
					dialog.ShowError(fmt.Errorf("Error deleting scan state: %v", err), myWindow)
				}
			}
		}, myWindow)
	}

	// Layout
	topForm := container.NewVBox(
		widget.NewLabel("Connect to SQL Server"),
		serverEntry,
		portEntry,
		usernameEntry,
		passwordEntry,
		dbNameEntry,
		connectButton,
		createTableButton,
		selectTableButton,
	)

	middleForm := container.NewHBox(
		folderEntry,
		browseButton,
		manualPathButton,
	)

	bottomForm := container.NewHBox(
		startButton,
		pauseButton,
		resumeButton,
		stopButton,
	)

	content := container.NewVBox(
		topForm,
		widget.NewSeparator(),
		widget.NewLabel("Enter or Select Folder to Scan"),
		middleForm,
		widget.NewSeparator(),
		bottomForm,
		statusLabel,
		progressLabel,
		widget.NewLabel("Log:"),
		logViewer,
	)

	// Start log update routine
	go func() {
		for logMsg := range logChan {
			logViewer.SetText(logViewer.Text + logMsg + "\n")
		}
	}()

	myWindow.SetContent(content)
	myWindow.Resize(fyne.NewSize(600, 800))
	myWindow.ShowAndRun()
}

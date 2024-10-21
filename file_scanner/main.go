package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"strings"
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
	logFile       *os.File
)

type multiWriter struct {
	writers []io.Writer
}

func (mw *multiWriter) Write(p []byte) (n int, err error) {
	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
	}
	return len(p), nil
}

func main() {
	var err error
	logFile, err = os.OpenFile("file_scanner.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer logFile.Close()

	mw := &multiWriter{
		writers: []io.Writer{os.Stdout, logFile},
	}
	log.SetOutput(mw)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recovered: %v\nStack Trace:\n%s", r, debug.Stack())
		}
	}()

	log.Println("Application started")

	myApp := app.New()
	myWindow := myApp.NewWindow("File Scanner")

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

	if err := loadCredentials(serverEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry); err != nil {
		log.Printf("Error loading credentials: %v", err)
		dialog.ShowError(err, myWindow)
	}

	statusLabel := widget.NewLabel("Status: Not connected")
	progressLabel := widget.NewLabel("Progress: Not started")

	connectButton := widget.NewButton("Connect", nil)
	createTableButton := widget.NewButton("Create New Table", nil)
	selectTableButton := widget.NewButton("Select Existing Table", nil)

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
				log.Printf("Selected folder path: %s", uri.Path())
			}
		}, myWindow)
	})

	manualPathButton := widget.NewButton("Enter UNC Path", func() {
		entry := widget.NewEntry()
		entry.SetPlaceHolder("Enter UNC path (e.g., \\\\server\\share)")
		dialog.ShowCustomConfirm("Enter UNC Path", "OK", "Cancel", entry, func(b bool) {
			if b {
				folderEntry.SetText(entry.Text)
				log.Printf("Manually entered folder path: %s", entry.Text)
			}
		}, myWindow)
	})

	startButton := widget.NewButton("Start Scan", nil)
	pauseButton := widget.NewButton("Pause Scan", nil)
	resumeButton := widget.NewButton("Resume Scan", nil)
	stopButton := widget.NewButton("Stop Scan", nil)

	createTableButton.Disable()
	selectTableButton.Disable()
	startButton.Disable()
	pauseButton.Disable()
	resumeButton.Disable()
	stopButton.Disable()

	var db *sql.DB
	var tableName string

	connectButton.OnTapped = func() {
		server := serverEntry.Text
		port := portEntry.Text
		if port == "" {
			port = "1433"
		}
		username := usernameEntry.Text
		password := passwordEntry.Text
		dbName := dbNameEntry.Text

		connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
			server, username, password, port, dbName)

		var err error
		db, err = sql.Open("sqlserver", connString)
		if err != nil {
			log.Printf("Error opening database connection: %v", err)
			statusLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}

		err = db.Ping()
		if err != nil {
			log.Printf("Error pinging database: %v", err)
			statusLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}

		if err := saveCredentials(serverEntry, portEntry, usernameEntry, passwordEntry, dbNameEntry); err != nil {
			log.Printf("Error saving credentials: %v", err)
			dialog.ShowError(err, myWindow)
		}

		log.Println("Database connected successfully")
		statusLabel.SetText("Status: Connected successfully")
		createTableButton.Enable()
		selectTableButton.Enable()
	}

	createTableButton.OnTapped = func() {
		entry := widget.NewEntry()
		entry.SetPlaceHolder("Enter New Table Name")
		dialog.ShowCustomConfirm("Create New Table", "Create", "Cancel", entry, func(b bool) {
			if b {
				tableName = entry.Text
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

	startButton.OnTapped = func() {
		folderPath := strings.TrimSpace(folderEntry.Text)
		if folderPath == "" {
			log.Println("Error: No folder path provided")
			statusLabel.SetText("Error: Please enter or select a folder path to scan")
			return
		}

		if _, err := os.Stat(folderPath); os.IsNotExist(err) {
			log.Printf("Error: Folder path does not exist: %s", folderPath)
			statusLabel.SetText(fmt.Sprintf("Error: Folder path does not exist: %s", folderPath))
			return
		}

		scanning = true
		paused = false
		startButton.Disable()
		pauseButton.Enable()
		stopButton.Enable()
		statusLabel.SetText("Status: Scanning")

		if err := loadScanState(); err != nil {
			log.Printf("Error loading scan state: %v", err)
			dialog.ShowError(fmt.Errorf("Error loading scan state: %v", err), myWindow)
		}

		scanState.FolderPath = folderPath
		if scanState.FilesScanned == nil {
			scanState.FilesScanned = make(map[string]bool)
		}

		var ctx context.Context
		ctx, cancelFunc = context.WithCancel(context.Background())
		scanDone = make(chan struct{})

		go func() {
			defer close(scanDone)
			log.Printf("Starting scan of folder: %s", folderPath)
			err := scanFolder(ctx, db, tableName, folderPath)
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

		ticker := time.NewTicker(100 * time.Millisecond)
		go func() {
			for {
				select {
				case <-ticker.C:
					if !scanning {
						ticker.Stop()
						return
					}
					filesScanned, filesWritten, scanSpeed, writeSpeed := GetProgressStats()
					progressText := fmt.Sprintf("Progress: Scanned %d files, Written %d files\nScan speed: %.2f files/sec, Write speed: %.2f files/sec",
						filesScanned, filesWritten, scanSpeed, writeSpeed)
					log.Println(progressText)
					progressLabel.SetText(progressText)
					myWindow.Canvas().Refresh(progressLabel)
				case <-scanDone:
					ticker.Stop()
					return
				}
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
			myWindow.Content().Refresh()
		}()
	}

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
	)

	myWindow.SetContent(content)
	myWindow.Resize(fyne.NewSize(600, 600))
	myWindow.ShowAndRun()
}

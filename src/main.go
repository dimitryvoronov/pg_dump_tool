package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfig struct {
	DBHostname string `yaml:"db_hostname"`
	DBUser     string `yaml:"db_user"`
	DBPassword string `yaml:"db_password"`
	DBPort     int    `yaml:"db_port"`
}

type RotationConfig struct {
	PeriodDays    int `yaml:"period_days"`
	RetentionDays int `yaml:"retention_days"`
}

type Config struct {
	DataPath string         `yaml:"data_path"`
	Rotation RotationConfig `yaml:"rotation"`
	Servers  []ServerConfig `yaml:"servers"`
}

func main() {
	// Add Flag for db hostname
	var dbHostname string
	flag.StringVar(&dbHostname, "h", "", "Database hostname")
	flag.Parse()

	if dbHostname == "" {
		fmt.Println("Usage: ", os.Args[0], "-h db-hostname")
		os.Exit(1)
	}

	execPath, err := os.Executable()
	if err != nil {
		log.Fatalf("Error getting executable path for %v", err)
	}
	execDir := filepath.Dir(execPath)
	configFile := filepath.Join(execDir, fmt.Sprintf("%s-config.yml", dbHostname))

	// Read configuration from YAML file
	config := Config{}
	yamlFile, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
	}

	// Iterate through servers in the config
	for _, server := range config.Servers {
		// Set PGPASSWORD environment variable
		os.Setenv("PGPASSWORD", server.DBPassword)

		// Determine log file path
		lodDir := filepath.Join(config.DataPath, "dump-logs")
		logFilePath := filepath.Join(lodDir, fmt.Sprintf("%s-sql-dmp-%s.log", server.DBHostname, time.Now().Format("20060102-150405")))

		// Setup logging to file
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)

		// Log start of the process with timestamp
		log.Printf("Starting backup for %s\n", server.DBHostname)

		// Try connecting via both ports (5432 and 6432)
		var databases []string
		var connPort int
		for _, port := range []int{server.DBPort, 6432} {
			db, err := listDatabases(server, port)
			if err == nil {
				databases = db
				connPort = port
				break
			}
			log.Printf("Error connecting to %s on port %d: %v", server.DBHostname, port, err)
		}

		if len(databases) == 0 {
			log.Fatalf("Error connecting to %s: unable to connect via any port", server.DBHostname)
		}

		// List databases
		if err != nil {
			log.Fatalf("Error listing databases on %s: %v", server.DBHostname, err)
		}
		log.Printf("Databases list is: %s \n", databases)

		// Dump databases (excluding postgres, template0, template1)
		for _, db := range databases {
			if db != "postgres" && db != "template0" && db != "template1" {
				// define subdir for /data (general location)
				currentDate := time.Now().Format("2006-01-02")
				dumpDir := filepath.Join(config.DataPath, currentDate)

				if err := os.MkdirAll(dumpDir, 0755); err != nil {
					log.Fatalf("Error creating dump directory: %v", err)
				}

				dumpFile := filepath.Join(dumpDir, fmt.Sprintf("%s-%s-%s", server.DBHostname, db, time.Now().Format("20060102-150405")))
				dumpCmd := fmt.Sprintf("pg_dump -j 4 -Fd -h %s -d %s -U %s -p %d -w -f %s", server.DBHostname, db, server.DBUser, connPort, dumpFile)
				log.Printf("DumpCmd is: %s \n", dumpCmd)
				dmp := exec.Command("sh", "-c", dumpCmd)
				var stdout, stderr bytes.Buffer
				dmp.Stdout = &stdout
				dmp.Stderr = &stderr
				if err := dmp.Run(); err != nil {
					log.Fatalf("Error dumping database %s for %s : %v\n", db, server.DBHostname, err)
					log.Fatalf("Stderr: %s\n", stderr.String())
				}

				log.Printf("Database dump successful for %s for %s\n", db, server.DBHostname)
				log.Println("Dump file is:", dumpFile)
			}
		}

		// Remove old files
		if err := removeOldFiles(config.DataPath, config.Rotation.RetentionDays); err != nil {
			log.Printf("Error removing old files: %v", err)
		}
		log.Printf("Old files removed for %s\n", server.DBHostname)

		// Log process completion
		log.Printf("Backup process completed for %s\n", server.DBHostname)
	}
}

func listDatabases(server ServerConfig, port int) ([]string, error) {
	cmd := exec.Command("psql", "-h", server.DBHostname, "-U", server.DBUser, "-p", fmt.Sprintf("%d", port), "-d", "postgres", "-t", "-c", "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1')")
	log.Println("Full pg_dump command is: \n", cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	databases := strings.Split(out.String(), "\n")
	var cleanedDatabases []string
	for _, db := range databases {
		if db != "" {
			cleanedDatabases = append(cleanedDatabases, strings.TrimSpace(db))
		}
	}
	return cleanedDatabases, nil
}

func archiveFiles(archiveDir string) error {
	archiveFile := fmt.Sprintf("%s.tar.gz", archiveDir)

	// Create a new tar.gz archive file
	tarGzFile, err := os.Create(archiveFile)
	if err != nil {
		return err
	}
	defer tarGzFile.Close()

	// Create a new gzip writer
	gzipWriter := gzip.NewWriter(tarGzFile)
	defer gzipWriter.Close()

	// Create a new tar writer
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	// Walk through the directory and add files to the tar.gz archive
	err = filepath.Walk(archiveDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get the relative path inside the archive
		relPath, err := filepath.Rel(archiveDir, path)
		if err != nil {
			return err
		}

		// Create a new tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		// Write the file header to the tar.gz archive
		err = tarWriter.WriteHeader(header)
		if err != nil {
			return err
		}

		// If the file is not a directory, write its content to the tar.gz archive
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			_, err = io.Copy(tarWriter, file)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func removeOldFiles(dataPath string, retentionDays int) error {
	return filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != dataPath {
			// If it's a directory (excluding the base directory), check if it's older than retentionDays
			if time.Since(info.ModTime()).Hours() > float64(retentionDays*24) {
				log.Printf("Removing old directory: %s\n", path)
				return os.RemoveAll(path)
			}
			return filepath.SkipDir // Skip subdirectories within the base directory
		} else if !info.IsDir() {
			// If it's a file, check if it's older than retentionDays
			if time.Since(info.ModTime()).Hours() > float64(retentionDays*24) {
				log.Printf("Removing old file: %s\n", path)
				return os.Remove(path)
			}
		}
		return nil
	})
}

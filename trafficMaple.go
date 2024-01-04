package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "strconv"
	"strings"
)

// Map function: Processes each line of a csv file and maps to (field, (dataset, line))
func Map(filename, prefix, dir, interconneValue string, startLine, endLine int) {
    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

	interconne := "Interconne"
	detection := "Detection_"


    // Read the first line to find the column index of the field
    var interconneIndex = -1
	var detectionIndex = -1
    if scanner.Scan() {
        header := scanner.Text()
        columns := strings.Split(header, ",")
        for i, col := range columns {
            if col == interconne {
                interconneIndex = i
            } else if col == detection {
				detectionIndex = i
			}
        }
    }

	if interconneIndex == -1 {
        fmt.Printf("Field '%s' not found in the file\n", interconne)
        return
    }

	if detectionIndex == -1 {
		fmt.Printf("Field '%s' not found in the file\n", detection)
        return
	}

    currentLine := 1 // Start from 1 as we already read the first line
    for scanner.Scan() {
        // Only process lines within the specified range
        if currentLine >= startLine && currentLine <= endLine {
            line := scanner.Text()
            values := strings.Split(line, ",")

            if len(values) > interconneIndex && len(values) > detectionIndex {
                interconneEntry := values[interconneIndex] // value of the column corresponding to the field

				// if this entry has the matching interconne value, map line to (detection field, 1)
				if interconneEntry == interconneValue {
					fmt.Println(line)
					detectionEntry := values[detectionIndex]
					fmt.Println(detectionEntry)
					logToFile(prefix, detectionEntry, "1", dir)
				}
            }
        }
        currentLine++
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error scanning file:", err)
    }
}

// logToFile writes the line to a file named with the prefix in the designated directory.
func logToFile(prefix, key, line, dir string) {
	key = strings.Replace(key, "/", "-", -1)
    fileName := filepath.Join(dir, prefix + "_" + key)
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println("Error opening log file:", err)
        return
    }
    defer file.Close()

    if _, err := file.WriteString(line + "\n"); err != nil {
        fmt.Println("Error writing to log file:", err)
    }
}

func main() {
    if len(os.Args) < 7 {
        fmt.Println("Usage: map_phase <directory> <prefix> <file> <startLine> <endLine> <interconneValue>")
        os.Exit(1)
    }

    dir := os.Args[1]
    prefix := os.Args[2]
    interconneValue := os.Args[6]

    // Parse startLine and endLine from command line arguments
    startLine, err := strconv.Atoi(os.Args[4])
    if err != nil {
        fmt.Println("Invalid start line number:", err)
        os.Exit(1)
    }

    endLine, err := strconv.Atoi(os.Args[5])
    if err != nil {
        fmt.Println("Invalid end line number:", err)
        os.Exit(1)
    }

    // Create the directory if it doesn't exist
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        err := os.Mkdir(dir, 0755)
        if err != nil {
            fmt.Println("Error creating directory:", err)
            return
        }
    }
    Map(os.Args[3], prefix, dir, interconneValue, startLine, endLine)
}

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
func Map(filename, prefix, dir, field, joinPosition string, startLine, endLine int) {
    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

    // Read the first line to find the column index of the field
    var columnIndex = -1
    if scanner.Scan() {
        header := scanner.Text()
        columns := strings.Split(header, ",")
        for i, col := range columns {
            if col == field {
                columnIndex = i
                break
            }
        }
    }

	if columnIndex == -1 {
        fmt.Printf("Field '%s' not found in the file\n", field)
        return
    }

    currentLine := 1 // Start from 1 as we already read the first line
    for scanner.Scan() {
        // Only process lines within the specified range
        if currentLine >= startLine && currentLine <= endLine {
            line := scanner.Text()
            values := strings.Split(line, ",")
            if len(values) > columnIndex {
                key := values[columnIndex] // value of the column corresponding to the field
				fmt.Println(key)
				value := joinPosition + "," + line
                logToFile(prefix, key, value, dir)
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
        fmt.Println("Usage: map_phase <directory> <prefix> <file> <startLine> <endLine> <field> <key>")
        os.Exit(1)
    }

    dir := os.Args[1]
    prefix := os.Args[2]
    field := os.Args[6]
    joinPosition := os.Args[7]

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
    Map(os.Args[3], prefix, dir, field, joinPosition, startLine, endLine)
}

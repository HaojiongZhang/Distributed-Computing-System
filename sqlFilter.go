package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
)

// Filter function: Processes the file and logs lines that match the regexp within the specified line range.
func Filter(filename, prefix, dir, regexpStr string, startLine, endLine int) {
    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    // Compile the regular expression
    re, err := regexp.Compile(regexpStr)
    if err != nil {
        fmt.Println("Error compiling regular expression:", err)
        return
    }

    scanner := bufio.NewScanner(file)
    currentLine := 0
    for scanner.Scan() {
        // Only process lines within the specified range
        if currentLine >= startLine && currentLine <= endLine {
            line := scanner.Text()
            // Check if the line matches the regular expression
            if re.MatchString(line) {
                logToFile(prefix, line, dir)
            }
        }
        currentLine++
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error scanning file:", err)
    }
}

// logToFile writes the line to a file named with the prefix in the designated directory.
func logToFile(prefix, line, dir string) {
    fileName := filepath.Join(dir, prefix)
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
        fmt.Println("Usage: map_phase <directory> <prefix> <file> <startLine> <endLine> <regexp> ")
        os.Exit(1)
    }

    dir := os.Args[1]
    prefix := os.Args[2]
    regexpStr := os.Args[6]

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
    Filter(os.Args[3], prefix, dir, regexpStr, startLine, endLine)

}

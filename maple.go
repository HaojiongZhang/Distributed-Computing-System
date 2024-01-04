package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
)

func Map(filename string, prefix string, dir string, startLine, endLine int, wg *sync.WaitGroup) {
    defer wg.Done()

    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    currentLine := 0

    for scanner.Scan() {
        if currentLine >= startLine && currentLine <= endLine {
            word := strings.ToLower(scanner.Text())
            logToFile(prefix, word, dir)
        }

        if currentLine > endLine {
            break // Stop scanning once the end line is passed
        }

        currentLine++
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}

func logToFile(prefix, word, dir string) {
    fileName := filepath.Join(dir, fmt.Sprintf("%s_%s", prefix, word))
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println("Error opening log file:", err)
        return
    }
    defer file.Close()

    if _, err := file.WriteString(word + "\n"); err != nil {
        fmt.Println("Error writing to log file:", err)
    }
}
func main() {

    if len(os.Args) != 6 {
        fmt.Println("Usage: go run maple.go <directory> <prefix> <file> <startLine> <endLine>")
        os.Exit(1)
    }

    dir := os.Args[1]
    prefix := os.Args[2]
    filename := os.Args[3]
    startLine, err := strconv.Atoi(os.Args[4])
    if err != nil {
        fmt.Println("Invalid start line:", os.Args[4])
        os.Exit(1)
    }
    endLine, err := strconv.Atoi(os.Args[5])
    if err != nil {
        fmt.Println("Invalid end line:", os.Args[5])
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


    var wg sync.WaitGroup
    wg.Add(1)
    go Map(filename, prefix, dir, startLine, endLine, &wg)
    wg.Wait()
}

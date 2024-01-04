package main

import (
    "bufio"
    "fmt"
    "os"
)

func main() {
    // Check if a filename is provided
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run [this_program.go] [filename]")
        os.Exit(1)
    }

    // Open the file
    filename := os.Args[1]
    file, err := os.Open(filename)
    if err != nil {
        fmt.Printf("Error opening file: %s\n", err)
        os.Exit(1)
    }
    defer file.Close()

    // Count the lines
    scanner := bufio.NewScanner(file)
    lineCount := 0
    for scanner.Scan() {
        lineCount++
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Error reading file: %s\n", err)
        os.Exit(1)
    }

    // Print the number of lines
    fmt.Println(lineCount)
}
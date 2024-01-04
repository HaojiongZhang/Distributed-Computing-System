package main

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
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

    // Read the file and sum the values
    scanner := bufio.NewScanner(file)
    total := 0
    keyValuePairs := make(map[string]int)
    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.Split(line, " : ")
        if len(parts) != 2 {
            fmt.Println("Invalid line format:", line)
            continue
        }
        key := strings.Replace(parts[0], "-", "/", -1) 
        value, err := strconv.Atoi(parts[1])
        if err != nil {
            fmt.Printf("Invalid value for key %s: %s\n", key, parts[1])
            continue
        }
        keyValuePairs[key] = value
        total += value
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Error reading file: %s\n", err)
        os.Exit(1)
    }

    // Print each key and its value as a percentage of the total
    if total > 0 {
        for key, value := range keyValuePairs {
            percentage := (float64(value) / float64(total)) * 100
            fmt.Printf("%s : %.6f%%\n", key, percentage)
        }
    } else {
        fmt.Println("Total sum of values is 0")
    }
}
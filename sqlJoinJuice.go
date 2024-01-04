package main

import (
    "bufio"
    "fmt"
    "os"
	"strings"
)

// Join function: Pairs each line that is from the left dataset with every other line from the right dataset 
func Join(filename string) {
    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    leftDataset := make([]string, 0)
    rightDataset := make([]string, 0)

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.SplitN(line, ",", 2)
        if len(parts) < 2 {
            continue 
        }

        dataset, values := parts[0], parts[1]

        if dataset == "left" {
            leftDataset = append(leftDataset, values)
        } else if dataset == "right" {
            rightDataset = append(rightDataset, values)
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error scanning file:", err)
        return
    }

    // Pair and print each combination of left and right datasets
    for _, leftValue := range leftDataset {
        for _, rightValue := range rightDataset {
            fmt.Println(leftValue + "," + rightValue)
        }
    }
}

func main() {
    // Check if a filename is provided	
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run [this_program.go] [filename]")
        os.Exit(1)
    }

    Join(os.Args[1])
}
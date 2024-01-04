package main

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
	"sync"
	"bufio"
	"encoding/json"
	"io/ioutil"
	"strings"
)

type FileArgs struct {
	Filename  string
	Timestamp time.Time
	IsAppend  bool
	IsFirst   bool
	IsLast    bool
	Data      []byte
}

type LineCountResult struct {
    Filename  string
    LineCount int
}

type PutFileArgs struct {
	Filename  string
	Timestamp time.Time
	IsAppend  bool
}

type FileService int

type AppendFileArgs struct {
    Filename  string
    Data      []byte
}


const (
	rpcPort = ":6666"
	storagePath = "storage/"
	// chunkSize = 1024*500 
	//chunkSize = 534288000
	chunkSize = 1356378933 
	putPort = ":5678"
	putDelimiter = '\n'
	putChunkSize = 5e+7
)

var connections = make(map[string]*rpc.Client)
var timestampMap = make(map[string]time.Time)
var rwMutex sync.RWMutex


// =========================================================   Server Side Code =============================================================================

func appendListener() {
	HostName, _ := os.Hostname()
    tcpAddr, err := net.ResolveTCPAddr("tcp4", HostName + ":7777") // Choose an appropriate address and port
    if err != nil {
        fmt.Println("Error resolving TCP address for append:", err)
        return
    }

    tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
    if err != nil {
        fmt.Println("Error listening for append:", err)
        return
    }
    defer tcpListener.Close()

    for {
        conn, err := tcpListener.Accept()
        if err != nil {
            fmt.Println("Error accepting append connection:", err)
            continue
        }
        go handleFileAppend(conn)
    }
}

// Handle File Append
func handleFileAppend(conn net.Conn) {
	rwMutex.Lock()  // Acquire a write lock
	defer rwMutex.Unlock()
	defer conn.Close()

	// Receive put args
	reader := bufio.NewReader(conn)
	argsBytes, err := reader.ReadBytes(putDelimiter)
	if err != nil {
		fmt.Println("Error receiving file args:", err)
		return
	}
	var fileArgs PutFileArgs
	err = json.Unmarshal(argsBytes, &fileArgs)
	if err != nil {
		fmt.Println("Error unmarshaling file args:", err)
		return
	}
	fmt.Printf("Received file args: %+v\n", fileArgs)

	// File to write content to
	filePath := filepath.Join(storagePath, fileArgs.Filename)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
    if err != nil {
        fmt.Println("Error opening file for append:", err)
        return
    }
	defer file.Close()

	// Read chunks of file contents
	buffer := make([]byte, putChunkSize)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from connection:", err)
			}
			break
		}

		chunk := buffer[:n]
		_, err = file.Write(chunk)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		// fmt.Println("Received chunk of length: ", n)
	}
	fmt.Println("All chunks received")

	// Update the timestamp map
	timestampMap[fileArgs.Filename] = fileArgs.Timestamp

	// Send confirmation
	_, err = conn.Write([]byte("All chunks received."))
	if err != nil {
		fmt.Println("Error sending confirmation:", err)
	} else {
		fmt.Println("Confirmation sent")
	}
    
}

func putListener() {
	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", HostName+putPort)
	tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer tcpListener.Close()

	for {
		conn, err := tcpListener.Accept()

		if !isPaused {
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				continue
			}
			go handleFilePut(conn)
		} 
	}
}

func handleFilePut(conn net.Conn) {
	rwMutex.Lock()  // Acquire a write lock
	defer rwMutex.Unlock()
	defer conn.Close()

	// Receive put args
	reader := bufio.NewReader(conn)
	argsBytes, err := reader.ReadBytes(putDelimiter)
	if err != nil {
		fmt.Println("Error receiving file args:", err)
		return
	}
	var fileArgs PutFileArgs
	err = json.Unmarshal(argsBytes, &fileArgs)
	if err != nil {
		fmt.Println("Error unmarshaling file args:", err)
		return
	}
	fmt.Printf("Received file args: %+v\n", fileArgs)

	// File to write content to
	filePath := filepath.Join(storagePath, fileArgs.Filename)

	var file *os.File 
	// Create or Append
	if (fileArgs.IsAppend == true) {
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		file, err = os.Create(filePath)
	}
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Read chunks of file contents
	buffer := make([]byte, putChunkSize)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from connection:", err)
			}
			break
		}

		chunk := buffer[:n]
		_, err = file.Write(chunk)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		// fmt.Println("Received chunk of length: ", n)
	}
	fmt.Println("All chunks received")

	// Update the timestamp map
	timestampMap[fileArgs.Filename] = fileArgs.Timestamp

	// Send confirmation
	_, err = conn.Write([]byte("All chunks received."))
	if err != nil {
		fmt.Println("Error sending confirmation:", err)
	} else {
		fmt.Println("Confirmation sent")
	}
}

func (t *FileService) Get(args *FileArgs, reply *FileArgs) error {
	rwMutex.RLock() // Acquire a read lock.
	defer rwMutex.RUnlock()

	filePath := filepath.Join(storagePath, args.Filename)
	
	// Open the file and seek to the position specified by args.Offset
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// Read a chunk of data from the file
	buffer := make([]byte, chunkSize)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return err
	}
	
	// Set the data and timestamp in the reply
	reply.Data = buffer[:n]
	reply.Timestamp = timestampMap[args.Filename]
	reply.IsLast = true
	return nil
}

func (t *FileService) Delete(filename string, reply *string) error {
	rwMutex.Lock()  // Acquire a write lock
	defer rwMutex.Unlock()

	filePath := filepath.Join(storagePath, filename)
	err := os.Remove(filePath)
	if err != nil {
		return err
	}
	delete(timestampMap, filename)
	*reply = "File deleted successfully"
	return nil
}

func startServer() {
	// Ensure storage directory exists
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		os.Mkdir(storagePath, 0755)
	}

	fileService := new(FileService)
	rpc.Register(fileService)
	l, e := net.Listen("tcp", rpcPort)
	if e != nil {
		panic(e)
	}
	fmt.Println("Server started on port 6666")
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}


func countLines(filePath string) (int, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return 0, err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    lineCount := 0
    for scanner.Scan() {
        lineCount++
    }
    return lineCount, scanner.Err()
}


func (t *FileService) GetLineCounts(prefixName string, reply *map[string]int) error {
    *reply = make(map[string]int)
	dirName := storagePath
    err := filepath.Walk(dirName, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() && strings.HasPrefix(info.Name(), prefixName) {
            count, err := countLines(path)
            if err != nil {
                return err
            }
            (*reply)[info.Name()] = count
        }
        return nil
    })
    return err
}

func (t *FileService) GetPrefixedFiles(prefixName string, reply *[]string) error {
    files, err := ioutil.ReadDir(storagePath)
    if err != nil {
        return err
    }

	for _, file := range files {
        if strings.HasPrefix(file.Name(), prefixName + "_") {
            *reply = append(*reply, file.Name()) // Append the file name to the reply
        }
    }
	
	return nil
}

// =========================================================   Client Side Code =============================================================================
func appendFile(fullpath string, hostname string) error {
    conn, err := net.Dial("tcp4", hostname+":7777")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return err
	}
	defer conn.Close()

	file, err := os.Open(fullpath)

	if err != nil {
		return err
	}
	defer file.Close()

	// Send file metadata
	filename := filepath.Base(fullpath)
	fileArgs := FileArgs{
		Filename:  filename,
		Timestamp: time.Now(),
	}

	argsBytes, err := json.Marshal(fileArgs)
	if err != nil {
		fmt.Println("Error marshaling file arg:", err)
		return err
	}
	_, err = conn.Write(argsBytes)
	if err != nil {
		fmt.Println("Error sending file args:", err)
		return err
	}

	// Signal end of arg message
	_, err = conn.Write([]byte(string(putDelimiter)))
	if err != nil {
		fmt.Println("Error sending end of JSON signal:", err)
		return err
	}

	buffer := make([]byte, putChunkSize)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from file:", err)
			}
			break
		}
		_, err = conn.Write(buffer[:n])
		if err != nil {
			fmt.Println("Error writing to connection:", err)
			return err
		}
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	} else {
		fmt.Println("Connection is not a TCP connection")
	}

	// Receive confirmation
	buffer = make([]byte, putChunkSize)
	n, err := conn.Read(buffer)
	if err != nil {
		if err != io.EOF {
			fmt.Println("Error reading confirmation:", err)
		}
		return err
	}
	fmt.Println("Received confirmation:", string(buffer[:n]))

	return nil
}


func getLineCounts(hostname string, dirName string) (map[string]int, error) {
    var lineCounts map[string]int
    err := callMethodOnServer(hostname, "FileService.GetLineCounts", dirName, &lineCounts)
    if err != nil {
        return nil, err
    }
    return lineCounts, nil
}

func getPrefixedFiles(hostname string, prefixName string) ([]string, error) {
	var prefixedFiles []string
	err := callMethodOnServer(hostname, "FileService.GetPrefixedFiles", prefixName, &prefixedFiles)
	if err != nil {
        return nil, err
    }
	return prefixedFiles, nil
}

// Connect to an RPC server and store the connection in the map
func connectToServer(hostname string) error {
	client, err := rpc.Dial("tcp", hostname+rpcPort)
	if err != nil {
		return err
	}
	connections[hostname] = client
	return nil
}

// Sample function to call a method on a specific server using its hostname
func callMethodOnServer(hostname string, method string, args interface{}, reply interface{}) error {
	client, exists := connections[hostname]
	if !exists {
		return fmt.Errorf("No connection found for hostname: %s", hostname)
	}
	return client.Call(method, args, reply)
}

func putFile(filename string, hostname string, isReplicating bool, isAppending bool) error {
	conn, err := net.Dial("tcp4", hostname+putPort)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return err
	}
	defer conn.Close()

	var filePath string
	// If replicating, we send file from storage
	// If a normal put, we send file from local
	if isReplicating {
		filePath = storagePath + filename
	} else {
		filePath = filename
	}
	file, err := os.Open(filePath)

	if err != nil {
		return err
	}
	defer file.Close()

	// Get base file name since we only support flat storage
	baseFileName := filepath.Base(filePath)

	// Send file metadata
	fileArgs := FileArgs{
		Filename:  baseFileName,
		Timestamp: time.Now(),
		IsAppend: isAppending,
	}

	argsBytes, err := json.Marshal(fileArgs)
	if err != nil {
		fmt.Println("Error marshaling file arg:", err)
		return err
	}
	_, err = conn.Write(argsBytes)
	if err != nil {
		fmt.Println("Error sending file args:", err)
		return err
	}

	// Signal end of arg message
	_, err = conn.Write([]byte(string(putDelimiter)))
	if err != nil {
		fmt.Println("Error sending end of JSON signal:", err)
		return err
	}

	buffer := make([]byte, putChunkSize)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from file:", err)
			}
			break
		}
		_, err = conn.Write(buffer[:n])
		if err != nil {
			fmt.Println("Error writing to connection:", err)
			return err
		}
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	} else {
		fmt.Println("Connection is not a TCP connection")
	}

	// Receive confirmation
	buffer = make([]byte, putChunkSize)
	n, err := conn.Read(buffer)
	if err != nil {
		if err != io.EOF {
			fmt.Println("Error reading confirmation:", err)
		}
		return err
	}
	fmt.Println("Received confirmation:", string(buffer[:n]))

	return nil
}

func getFile(filename string, hostnames []string) (time.Time, error) {
    var latestTimestamp time.Time
    var latestFileData []byte
	// var lastHostname string
    for _, hostname := range hostnames {
        timestamp, fileData, err := getFileFromHost(filename, hostname)
        if err != nil {
            continue  // Optionally, you could continue here to try other hosts
        }

        // Compare the timestamps and keep the file data with the latest timestamp
        if timestamp.After(latestTimestamp) {
            latestTimestamp = timestamp
            latestFileData = fileData
			// lastHostname = strings.Clone(hostname)
        }
    }
    // Now store the file data with the latest timestamp
    filePath := filepath.Join(filename)
    // err := ioutil.WriteFile(filePath, latestFileData, 0644)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
    if err != nil {
        return time.Time{}, err
    }
	_, err = file.Write(latestFileData)
	if err != nil {
        return time.Time{}, err
    }
    return latestTimestamp, nil
}

// This function retrieves a file from a single host and returns the timestamp and file data
func getFileFromHost(filename string, hostname string) (time.Time, []byte, error) {
    var offset int64 = 0
    var timestamp time.Time
    var fileData []byte

    for {
        args := &FileArgs{
            Filename: filename,
        }

        var reply FileArgs
        err := callMethodOnServer(hostname, "FileService.Get", args, &reply)
        if err != nil {
            return time.Time{}, nil, err
        }

        // If this is the first chunk, set the timestamp
        if offset == 0 {
            timestamp = reply.Timestamp
        }

        // Append the received chunk to fileData
        fileData = append(fileData, reply.Data...)

        // If this is the last chunk, break
        if reply.IsLast {
            break
        }

        offset += int64(len(reply.Data))
    }

    return timestamp, fileData, nil
}

// Delete a file from the server
func deleteFile(filename string, hostname string) error {
	var reply string
	err := callMethodOnServer(hostname, "FileService.Delete", filename, &reply)
	if err != nil {
		return err
	}

	fmt.Println("Reply:", reply)
	return nil
}

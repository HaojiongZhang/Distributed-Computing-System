package main

import (
    "fmt"
    "strings"
	"bufio"
    "sync"
    "encoding/json"
    "os/exec"
    "io"
    "net"
    "sort"
    "strconv"
	"os"
    "path/filepath"
    "time"
    "hash/fnv"
)

const (
	LeaderHostname = "fa23-cs425-3207.cs.illinois.edu"
	leaderPort = ":4321"
    mjPort = ":1234"
    juicePort = ":2346"
)

var (
    tasksMutex      sync.Mutex
    assignedJuiceTasks   []AssignedJuiceTask
)

type MapleTask struct {
    Filename    string
    StartLine   int 
    EndLine     int 
}


type JuiceTask struct {
    Filenames []string
    JuiceExe string
    SdfsIntermediateFilename string
    SdfsDestFilename string
    OutputMode string
    Parameters []string
}

type AssignedJuiceTask struct {
    Client   ClientInfo
    Task     JuiceTask
    Complete bool
}

type EnhancedMapleTask struct {
    Tasks                        []MapleTask
    MapleExe                     string
    SdfsIntermediateFilename string
    Parameters               []string
}

type MapleCommand struct {
    Command                    string
    MapleExe                   string
    NumMaples                  string
    SdfsIntermediateFilename   string
    SdfsSrcDirectory           string
    Parameters               []string
}


type JuiceCommand struct {
    Command                    string
    JuiceExe                   string
    NumJuices                  string
    SdfsIntermediateFilename   string
    SdfsDestFilename           string
    DeleteInput                string
    PartitionMode              string
    OutputMode                 string
    Parameters               []string
}

type SQLFilterCommand struct {
    Command                    string
    SdfsSrcDirectory           string
    Regexp                     string
    SdfsDestFilename           string
}

type SQLJoinCommand struct {
    Command                    string
    SdfsLeftSrcDirectory       string
    SdfsRightSrcDirectory      string
    LeftJoinField              string
    RightJoinField             string
    SdfsDestFilename           string
}

type TrafficCommand struct {
    Command                    string
    SdfsSrcDirectory           string
    Value                     string
    SdfsDestFilename           string
}

type CommandPacket struct {
    CmdName string      `json:"cmdName"`
    CmdData interface{} `json:"cmdData"`
}

// ------------------------------------------- sender portion ------------------------------------------------
func parseInput(input string) (*CommandPacket, error) {
    parts := strings.Fields(input)

    // Create a command packet
    var packet CommandPacket

    // Check the command type and parse accordingly
    switch parts[0] {
    case "maple":
        if len(parts) < 5 {
            return nil, fmt.Errorf("invalid maple command format")
        }
        packet = CommandPacket{
            CmdName: parts[0],
            CmdData: &MapleCommand{
                Command: parts[0],
                MapleExe: parts[1],
                NumMaples: parts[2],
                SdfsIntermediateFilename: parts[3],
                SdfsSrcDirectory: parts[4],
                Parameters: parts[5:],
            },
        }
    case "juice":
        if len(parts) != 6 {
            return nil, fmt.Errorf("invalid juice command format")
        }

        // Default to hash partition
        PartitionMode := "0"
        if len(parts) == 7 {
            PartitionMode = parts[6]
        }

        packet = CommandPacket{
            CmdName: parts[0],
            CmdData: &JuiceCommand{
                Command: parts[0],
                JuiceExe: parts[1],
                NumJuices: parts[2],
                SdfsIntermediateFilename: parts[3],
                SdfsDestFilename: parts[4],
                DeleteInput: parts[5],
                PartitionMode: PartitionMode,
            },
        }
    case "sql_filter":
        if len(parts) != 4 {
            return nil, fmt.Errorf("invalid sql filter command format")
        }
        packet = CommandPacket{
            CmdName: parts[0],
            CmdData: &SQLFilterCommand{
                Command: parts[0],
                SdfsSrcDirectory: parts[1],
                Regexp: parts[2],
                SdfsDestFilename: parts[3],
            },
        }
    case "sql_join":
        if len(parts) != 6 {
            return nil, fmt.Errorf("invalid sql join command format")
        }
        packet = CommandPacket{
            CmdName: parts[0],
            CmdData: &SQLJoinCommand{
                Command: parts[0],
                SdfsLeftSrcDirectory: parts[1],
                SdfsRightSrcDirectory: parts[2],
                LeftJoinField: parts[3],
                RightJoinField: parts[4],
                SdfsDestFilename: parts[5],
            },
        }
    case "traffic":
        if len(parts) != 4 {
            return nil, fmt.Errorf("invalid traffic command format")
        }
        packet = CommandPacket{
            CmdName: parts[0],
            CmdData: &TrafficCommand{
                Command: parts[0],
                SdfsSrcDirectory: parts[1],
                Value: parts[2],
                SdfsDestFilename: parts[3],
            },
        }
    default:
        return nil, fmt.Errorf("unknown command")
    }

    return &packet, nil
}


func callLeader(cmd *CommandPacket){
	
	jsonData, err := json.Marshal(cmd)
    if err != nil {
        fmt.Println("Error encoding command packet to JSON:", err)
        return
    }

    if err != nil {
        fmt.Println("Error encoding ClientList to JSON:", err)
        return
    }

    // Resolve the TCP address
    tcpAddr, err := net.ResolveTCPAddr("tcp4", LeaderHostname+leaderPort)
    if err != nil {
        fmt.Println("Error resolving address:", err)
        return
    }

    // Create a TCP connection to send data
    conn, err := net.DialTCP("tcp4", nil, tcpAddr)
    if err != nil {
        fmt.Println("Error creating TCP connection:", err)
        return
    }
    defer conn.Close()

	// Send the JSON data
    _, err = conn.Write(jsonData)
    if err != nil {
        fmt.Println("Error sending data:", err)
        return
    }

    fmt.Println("Command packet sent successfully")
   
}

// --------------------------------- receiver -----------------------------------------

func partitionWorkByTotalCount(lineCounts map[string]int, numMaples int, totalCount int) [][]MapleTask {
    partitions := make([][]MapleTask, numMaples)
    if numMaples == 0 || totalCount == 0 {
        return partitions
    }

    linesPerMaple := totalCount / numMaples
    extraLines := totalCount % numMaples

    var filenames []string
    for filename := range lineCounts {
        filenames = append(filenames, filename)
    }
    sort.Strings(filenames) // Sort filenames to ensure consistent ordering

    currentMaple := 0
    remainingLinesForMaple := linesPerMaple

    if extraLines > 0 {
        remainingLinesForMaple++ // Assign one extra line to the first maple
        extraLines--
    }

    for _, filename := range filenames {
        lineCount := lineCounts[filename]
        startLine := 0

        for lineCount > 0 {
            linesToAssign := min(lineCount, remainingLinesForMaple)
            endLine := startLine + linesToAssign - 1

            partitions[currentMaple] = append(partitions[currentMaple], MapleTask{
                Filename:  filename,
                StartLine: startLine,
                EndLine:   endLine,
            })

            lineCount -= linesToAssign
            remainingLinesForMaple -= linesToAssign
            startLine = endLine + 1

            if remainingLinesForMaple == 0 {
                currentMaple++
                if currentMaple < numMaples {
                    remainingLinesForMaple = linesPerMaple
                    if extraLines > 0 {
                        remainingLinesForMaple++
                        extraLines--
                    }
                }
            }
        }
    }

    return partitions
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}


func executeMapleCommand(cmd *MapleCommand) {
    fmt.Printf("Executing Maple Command: %+v\n", cmd)
    totalCount := 0
    aggregatedLineCounts := make(map[string]int)

    var clients []ClientInfo

	// query every client for a filename-time map for the directory
    for _, clientInfo := range ClientList {

        lineCounts, err := getLineCounts(clientInfo.Hostname, cmd.SdfsSrcDirectory)
        if err != nil {
            fmt.Println("Error getting line counts:", err)
            continue
        }

        clients = append(clients, clientInfo)

        // Merge line counts into the aggregated map
        for filename, count := range lineCounts {
            
            if value, exists := aggregatedLineCounts[filename]; exists {
                if value != count{
                    fmt.Println("maple received mismatch for file: ",filename)
                }
            } else {
                aggregatedLineCounts[filename] = count
                totalCount += count
            }
        }
    }
    // Print the aggregated line counts
    fmt.Println("Aggregated line counts in the directory:")
    for filename, count := range aggregatedLineCounts {
        fmt.Printf("%s: %d lines\n", filename, count)
        
    }

    // partition the map 
    numMaples, _ := strconv.Atoi(cmd.NumMaples) 
    if (len(clients) < numMaples) {
        numMaples = len(clients)
    }
    
    mapleTasks := partitionWorkByTotalCount(aggregatedLineCounts, numMaples, totalCount)

    // fmt.Println("======================")
    // fmt.Println(mapleTasks)
    // fmt.Println("======================")


    // Send tasks to individual maples
    var connections []net.Conn
    var taskMap []EnhancedMapleTask

    for mapleIndex, tasks := range mapleTasks {
        hostname := clients[mapleIndex].Hostname
        enhancedTask := EnhancedMapleTask{
            Tasks:                         tasks,
            MapleExe:                      cmd.MapleExe,
            SdfsIntermediateFilename: cmd.SdfsIntermediateFilename,
            Parameters: cmd.Parameters,
        }
    
        jsonData, err := json.Marshal(enhancedTask)
        if err != nil {
            fmt.Println("Error marshaling enhanced tasks:", err)
            continue
        }

        conn, err := net.Dial("tcp", hostname+mjPort)
        if err != nil {
            fmt.Println("Error dialing maple:", err)
            connections = append(connections, nil) // Store the connection for later
            taskMap = append(taskMap, enhancedTask)
            continue
        }

        _, err = conn.Write(jsonData)
        if err != nil {
            fmt.Println("Error sending data to maple:", err)
            connections = append(connections, nil) // Store the connection for later
            taskMap = append(taskMap, enhancedTask)
        } else {
            connections = append(connections, conn) // Store the connection for later
            taskMap = append(taskMap, enhancedTask)
        }
    }

    fmt.Println("Connections here: ", connections, taskMap)

    // Wait for acknowledgments from all maples
    var wg sync.WaitGroup

    for idx, conn := range connections {
        ackBuffer := make([]byte, 1024)
        var err error
        var n int

        if conn != nil {
            n, err = conn.Read(ackBuffer)
        }

        if err != nil || conn == nil{
            fmt.Printf("Error receiving acknowledgment %v\n", err)
            wg.Add(1) // Increment the WaitGroup counter
            go func(task EnhancedMapleTask) {
                defer wg.Done() // Decrement the counter when the goroutine completes
                reassignMapleTask(task)
            }(taskMap[idx])
        } else {
            ackMessage := string(ackBuffer[:n])
            fmt.Println("Received acknowledgment:", ackMessage)
        }

        if conn != nil {
            conn.Close()
        }
    }
    wg.Wait()
    fmt.Println("Maple task complete")
}

func reassignMapleTask(task EnhancedMapleTask) {
    for {
        clientListMutex.Lock()
        clientsCopy := make([]ClientInfo, len(ClientList))
        copy(clientsCopy, ClientList)
        clientListMutex.Unlock()

        for _, client := range clientsCopy {
            if sendTaskToClient(task, client.Hostname) {
                return // Task successfully reassigned and acknowledged
            }
            
        }

        time.Sleep(1 * time.Second) // Example delay before retrying
    }
}

func sendTaskToClient(task EnhancedMapleTask, hostname string) bool {
    jsonData, err := json.Marshal(task)
    if err != nil {
        fmt.Println("Error marshaling task:", err)
        return false
    }

    conn, err := net.Dial("tcp", hostname+mjPort)
    if err != nil {
        fmt.Printf("Error dialing maple %s: %v\n", hostname, err)
        return false
    }
    defer conn.Close()

    _, err = conn.Write(jsonData)
    if err != nil {
        fmt.Printf("Error sending task to %s: %v\n", hostname, err)
        return false
    }

    ackBuffer := make([]byte, 1024)
    n, err := conn.Read(ackBuffer)
    if err != nil {
        fmt.Printf("Error receiving acknowledgment from %s: %v\n", hostname, err)
        return false
    }

    ackMessage := string(ackBuffer[:n])
    fmt.Printf("Received acknowledgment for reassign from %s: %s\n", hostname, ackMessage)
    return true
}



func executeJuiceCommand(cmd *JuiceCommand) {
    fmt.Printf("Executing Juice Command: %+v\n", cmd)

    clientListMutex.Lock()
    clientsCopy := make([]ClientInfo, len(ClientList))
    copy(clientsCopy, ClientList)
    clientListMutex.Unlock()

    uniqueFilenames := make(map[string]struct{})
    var clients []ClientInfo

    // Iterate over each client in the copied list and get prefixed files
    for _, client := range clientsCopy {
        files, err := getPrefixedFiles(client.Hostname, cmd.SdfsIntermediateFilename)
        if err != nil {
            fmt.Println("Error getting files from client:", err)
            continue
        }

        clients = append(clients, client)

        // Add files to the map to ensure uniqueness
        for _, file := range files {
            uniqueFilenames[file] = struct{}{}
        }
    }

    // Create JuiceTasks based on NumJuices
    numJuices, err := strconv.Atoi(cmd.NumJuices)
    if len(clients) < numJuices {
        numJuices = len(clients)
    }
    if err != nil {
        fmt.Println("Invalid NumJuices value:", err)
        return
    }

    PartitionMode, err := strconv.Atoi(cmd.PartitionMode)
    if err != nil {
        fmt.Println("Invalid PartitionMode value:", err)
        return
    }

    index := 0 
    juiceTasks := make([]JuiceTask, numJuices)
    for filename := range uniqueFilenames {
        if PartitionMode == 0 {
            // Hash partition
            index = HashPartition(ExtractKey(filename, cmd.SdfsIntermediateFilename), numJuices)
        } else {
            // Range partition
            index = RangePartition(ExtractKey(filename, cmd.SdfsIntermediateFilename), numJuices)
        }
        juiceTasks[index].Filenames = append(juiceTasks[index].Filenames, filename)
    }

    assignJuiceTasksToClients(juiceTasks, clients, cmd)

    fmt.Println(assignedJuiceTasks)

    for i := range assignedJuiceTasks {
        go sendJuiceTask(&assignedJuiceTasks[i])
    }

    waitForJuiceCompletion()

    // When complete delete intermediate files if flag

    DeleteInput, err := strconv.Atoi(cmd.DeleteInput)
    if err != nil {
        fmt.Println("Invalid DeleteInput value:", err)
        return
    }

    if DeleteInput == 1 {
        for filename := range uniqueFilenames {
            handleSendingFileCommand("delete " + filename)
        } 
    }

    fmt.Println("All tasks completed")
}

func HashPartition(key string, numJuiceTasks int) int {
    hasher := fnv.New32a()
    hasher.Write([]byte(key))
    hash := hasher.Sum32()

    return int(hash) % numJuiceTasks
}

func RangePartition(key string, numJuiceTasks int) int {
    if len(key) == 0 {
        return 0
    }
    return int(key[0]) % numJuiceTasks
}

func assignJuiceTasksToClients(juiceTasks []JuiceTask, clients []ClientInfo, cmd *JuiceCommand) {
    tasksMutex.Lock()
    for i, task := range juiceTasks {
        if len(task.Filenames) == 0 {
            continue
        }

        task.JuiceExe = cmd.JuiceExe
        task.SdfsDestFilename = cmd.SdfsDestFilename
        task.SdfsIntermediateFilename = cmd.SdfsIntermediateFilename
        task.OutputMode = cmd.OutputMode
        task.Parameters = cmd.Parameters

        client := clients[i%len(clients)] // Rotate through the client list
        assignedJuiceTasks = append(assignedJuiceTasks, AssignedJuiceTask{Client: client, Task: task, Complete: false})
    }
    tasksMutex.Unlock()
}

func sendJuiceTask(assignedTask *AssignedJuiceTask) error {
    jsonData, err := json.Marshal(assignedTask.Task)
    if err != nil {
        return err // Return the error if marshaling fails
    }

    // Attempt to dial the client
    conn, err := net.Dial("tcp4", assignedTask.Client.Hostname + juicePort)
    if err != nil {
		fmt.Println("Error connecting:", err)
        reassignTask(assignedTask)
		return err
	}
    defer conn.Close()

    // Send the task
    _, err = conn.Write(jsonData)
    if err != nil {
        fmt.Println("Error sending data to maple:", err)
    }

    // Wait for ACK
    response, err := bufio.NewReader(conn).ReadString('\n')
    if err != nil || response != "ACK\n" {
        fmt.Printf("Connection error or incomplete ACK from %s: %s\n", assignedTask.Client.Hostname, err)
        reassignTask(assignedTask)
        return err
    }

    // ACK received, mark task as complete
    tasksMutex.Lock()
    assignedTask.Complete = true
    tasksMutex.Unlock()

    return nil
}

func reassignTask(assignedTask *AssignedJuiceTask) {
    fmt.Println("Reassign ", assignedTask)

    clientListMutex.Lock()
    clientsCopy := make([]ClientInfo, len(ClientList))
    copy(clientsCopy, ClientList)
    clientListMutex.Unlock()

    var newClient *ClientInfo
    for _, client := range clientsCopy {
        if client.Hostname != assignedTask.Client.Hostname && !isClientAssigned(client.Hostname) {
            newClient = &client
            break
        }
    }

    if newClient == nil {
        fmt.Println("No completely free client for reassignment")
    }

    // if no one finished, assign to anyone alive
    for _, client := range clientsCopy {
        if client.Hostname != assignedTask.Client.Hostname {
            fmt.Println("Reassigning task to busy Client")
            newClient = &client
            break
        }
    }

    tasksMutex.Lock()
    assignedTask.Client = *newClient
    assignedTask.Complete = false
    tasksMutex.Unlock()

    go sendJuiceTask(assignedTask)
}

func isClientAssigned(hostname string) bool {
    for _, task := range assignedJuiceTasks {
        // ?? Do we need && !task.Complete
        if task.Client.Hostname == hostname && !task.Complete{
            return true
        }
    }
    return false
}

func waitForJuiceCompletion() {
    for {
        allTasksCompleted := true

        tasksMutex.Lock()
        for _, task := range assignedJuiceTasks {
            if !task.Complete {
                allTasksCompleted = false
                // No need to monitor each connection here since it's handled in dialAndSendTask
            }
        }
        tasksMutex.Unlock()

        if allTasksCompleted {
            break // Exit the loop if all tasks are complete
        }

        // Sleep for a short duration before checking again
        time.Sleep(2 * time.Second)
    }
    assignedJuiceTasks = []AssignedJuiceTask{}
}

func ExtractKey(filename, prefix string) string {
    // Create the split pattern
    splitPattern := prefix + "_"

    // Split the string
    parts := strings.SplitN(filename, splitPattern, 2)
    if len(parts) < 2 {
        fmt.Println("Invalid format or missing key in filename")
        return ""
    }

    return parts[1]
}

func executeSQLFilterCommand(sqlFilterCmd *SQLFilterCommand) {
    fmt.Printf("Executing SQL Filter Command: %+v\n", sqlFilterCmd)
    
    mapleCmd := MapleCommand{
        Command: "maple",
        MapleExe: "sqlFilter.go",
        NumMaples: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: sqlFilterCmd.SdfsDestFilename,
        SdfsSrcDirectory: sqlFilterCmd.SdfsSrcDirectory,
        Parameters: []string{sqlFilterCmd.Regexp},
    }

    executeMapleCommand(&mapleCmd)
}

func executeSQLJoinCommand(sqlJoinCmd *SQLJoinCommand) {
    fmt.Printf("Executing SQL Join Command: %+v\n", sqlJoinCmd)
    
    // Execute maple for each dataset
    leftMapleCmd := MapleCommand{
        Command: "maple",
        MapleExe: "sqlJoinMaple.go",
        NumMaples: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "sql_join",
        SdfsSrcDirectory: sqlJoinCmd.SdfsLeftSrcDirectory,
        Parameters: []string{sqlJoinCmd.LeftJoinField, "left"},
    }
    executeMapleCommand(&leftMapleCmd)

    rightMapleCmd := MapleCommand{
        Command: "maple",
        MapleExe: "sqlJoinMaple.go",
        NumMaples: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "sql_join",
        SdfsSrcDirectory: sqlJoinCmd.SdfsRightSrcDirectory,
        Parameters: []string{sqlJoinCmd.RightJoinField, "right"},
    }
    executeMapleCommand(&rightMapleCmd)

    // Execute juice to perform join
    juiceCmd := JuiceCommand{
        Command: "juice",
        JuiceExe: "sqlJoinJuice.go",
        NumJuices: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "sql_join",
        SdfsDestFilename: sqlJoinCmd.SdfsDestFilename,
        DeleteInput: "1",
        PartitionMode: "0",
        OutputMode: "1",
    }
    executeJuiceCommand(&juiceCmd)
}

func executeTrafficCommand(trafficCmd *TrafficCommand) {
    fmt.Printf("Executing traffic Command: %+v\n", trafficCmd)
    
    mapleCmd := MapleCommand{
        Command: "maple",
        MapleExe: "trafficMaple.go",
        NumMaples: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "traffic",
        SdfsSrcDirectory: trafficCmd.SdfsSrcDirectory,
        Parameters: []string{trafficCmd.Value},
    }

    executeMapleCommand(&mapleCmd)

    juiceCmd := JuiceCommand{
        Command: "juice",
        JuiceExe: "trafficJuice.go",
        NumJuices: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "traffic",
        SdfsDestFilename: "avg_intermediate.txt",
        DeleteInput: "1",
        PartitionMode: "0",
        OutputMode: "0",
    }
    executeJuiceCommand(&juiceCmd)

    percentJuiceCmd := JuiceCommand{
        Command: "juice",
        JuiceExe: "percentJuice.go",
        NumJuices: strconv.Itoa(len(ClientList)),
        SdfsIntermediateFilename: "avg",
        SdfsDestFilename: trafficCmd.SdfsDestFilename,
        DeleteInput: "1",
        PartitionMode: "0",
        OutputMode: "1",
    }
    executeJuiceCommand(&percentJuiceCmd)
}

func handleCommand(packet *CommandPacket) {
    if packet == nil {
        fmt.Println("Received a nil packet")
        return
    }

    fmt.Printf("Received command packet: %+v\n", packet)

    // Marshal CmdData back into JSON
    jsonData, err := json.Marshal(packet.CmdData)
    if err != nil {
        fmt.Println("Error marshalling CmdData back to JSON:", err)
        return
    }

    switch packet.CmdName {
    case "maple":
        cmd := &MapleCommand{}
        err := json.Unmarshal(jsonData, cmd)
        if err != nil {
            fmt.Println("Error unmarshalling MapleCommand:", err)
            return
        }
        fmt.Printf("Maple Command Details:\n")
        fmt.Printf("MapleExe: %s, NumMaples: %s, SdfsIntermediateFilename: %s, SdfsSrcDirectory: %s, Parameters: %s\n", 
                   cmd.MapleExe, cmd.NumMaples, cmd.SdfsIntermediateFilename, cmd.SdfsSrcDirectory, cmd.Parameters)
		executeMapleCommand(cmd)
        
    case "juice":
        cmd := &JuiceCommand{}
        err = json.Unmarshal(jsonData, cmd)
        if err != nil {
            fmt.Println("Error unmarshalling JuiceCommand:", err)
            return
        }
        fmt.Printf("Juice Command Details:\n")
        fmt.Printf("JuiceExe: %s, NumJuices: %s, SdfsIntermediateFilename: %s, SdfsDestFilename: %s, DeleteInput: %s\n", 
                   cmd.JuiceExe, cmd.NumJuices, cmd.SdfsIntermediateFilename, cmd.SdfsDestFilename, cmd.DeleteInput)
		executeJuiceCommand(cmd)
    case "sql_filter":
        cmd := &SQLFilterCommand{}
        err = json.Unmarshal(jsonData, cmd)
        if err != nil {
            fmt.Println("Error unmarshalling SQL Filter Command:", err)
            return
        }
        fmt.Printf("SQL Filter Command Details:\n")
        fmt.Printf("SdfsSrcDirectory: %s, regexp: %s, SdfsDestFilename: %s\n", 
                   cmd.SdfsSrcDirectory, cmd.Regexp, cmd.SdfsDestFilename)
        executeSQLFilterCommand(cmd)
    case "sql_join":
        cmd := &SQLJoinCommand{}
        err = json.Unmarshal(jsonData, cmd)
        if err != nil {
            fmt.Println("Error unmarshalling SQL Join Command:", err)
            return
        }
        fmt.Printf("SQL Join Command Details:\n")
        fmt.Printf("SdfsLeftSrcDirectory: %s, SdfsRightSrcDirectory: %s, LeftJoinField: %s, RightJoinField: %s, SdfsDestFilename: %s\n", 
                  cmd.SdfsLeftSrcDirectory, cmd.SdfsRightSrcDirectory, cmd.LeftJoinField, cmd.RightJoinField, cmd.SdfsDestFilename)
        executeSQLJoinCommand(cmd)
    case "traffic":
        cmd := &TrafficCommand{}
        err = json.Unmarshal(jsonData, cmd)
        if err != nil {
            fmt.Println("Error unmarshalling SQL Join Command:", err)
            return
        }
        fmt.Printf("Traffic Command Details:\n")
        fmt.Printf("SdfsSrcDirectory: %s, value: %s, SdfsDestFilename: %s\n", 
                   cmd.SdfsSrcDirectory, cmd.Value, cmd.SdfsDestFilename)
        executeTrafficCommand(cmd)
    default:
        fmt.Printf("Unknown command type: %s\n", packet.CmdName)
    }
}

// Function to start the TCP server
func startLeader() {
    // Listen for incoming connections
    listener, err := net.Listen("tcp", leaderPort)
    if err != nil {
        fmt.Println("Error starting TCP server:", leaderPort)
        return
    }
    defer listener.Close()

    fmt.Println("Leader is listening on port", leaderPort)

    for {
        // Accept a connection
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting connection:", err)
            continue
        }


        // Handle the connection in a new goroutine
        go handleConnection(conn)
    }
}

// Function to handle a connection
func handleConnection(conn net.Conn) {
    defer conn.Close()
    // Read the incoming data
    reader := bufio.NewReader(conn)
    data, err := io.ReadAll(reader)
	


    if err != nil {
        fmt.Println("Error reading data:", err)
        return
    }

    // Decode the JSON data into a CommandPacket
    var packet CommandPacket
    err = json.Unmarshal(data, &packet)
    if err != nil {
        fmt.Println("Error decoding JSON data:", err)
        return
    }

    // Handle the command packet
    handleCommand(&packet)
}

// ------------- Maple Handler ---------------
func startMapleServer() {
    ln, err := net.Listen("tcp", mjPort)
    if err != nil {
        fmt.Println("Error starting maple server:", leaderPort)
        return
    }
    defer ln.Close()

    for {
        conn, err := ln.Accept()
        if !isPaused {
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				continue
			}
			go handleMapleTask(conn)
		} 
    }
}

func sendDir(dirName string){
    files, err := os.ReadDir(dirName)
    if err != nil {
        fmt.Println("error reading directory ",err)
    }
    fmt.Println("sending tmp outputs")
    // for _, file := range files {
    //     fmt.Println(file.Name())
    // }

    for _, file := range files {
        if file.IsDir() {
            continue // Skip directories
        }

        filePath := filepath.Join(dirName, file.Name())
        filePath = "append " + filePath
        handleSendingFileCommand(filePath)
    }
}


func handleMapleTask(conn net.Conn) {
    defer conn.Close()

    var task EnhancedMapleTask
    decoder := json.NewDecoder(conn)
    err := decoder.Decode(&task)
    if err != nil {
        fmt.Println("Error decoding enhanced tasks:", err)
        return
    }

    fmt.Printf("Received tasks: %+v\n", task.Tasks)
    fmt.Printf("MapleExe: %s, SdfsIntermediateFilenamePrefix: %s\n", task.MapleExe, task.SdfsIntermediateFilename)

    //handle maple task locally
    dir := "maple_tmp" + string(time.Now().Format("5.000")) // Replace with your actual output directory
    prefix := task.SdfsIntermediateFilename
    
    //get the maple task
    input := "get " + task.MapleExe
    handleSendingFileCommand(input)

    for _, mapleTask := range task.Tasks {
        //fetch the required files locally
        input := "get " + mapleTask.Filename
        handleSendingFileCommand(input)

        additionalArgs := []string{"run", task.MapleExe, dir, prefix, mapleTask.Filename, strconv.Itoa(mapleTask.StartLine), strconv.Itoa(mapleTask.EndLine)}
        additionalArgs = append(additionalArgs, task.Parameters...)

        fmt.Println(additionalArgs)

        // Construct the command to execute maple.go
        cmd := exec.Command("go", additionalArgs...)

        // Run the command  (maybe you could do this concurrently? critical section)
        out, err := cmd.CombinedOutput()
        fmt.Println(string(out))
        if err != nil {
            fmt.Printf("Error running %s for file %s: %v: %s\n", task.MapleExe, mapleTask.Filename, err, out)
        }
    }

    // sending map result
    sendDir(dir)

    // delete tmp directory
    err = os.RemoveAll(dir)
    if err != nil {
        fmt.Println("Error removing directory",err)
    }


    _, err = conn.Write([]byte("Tasks completed"))
    if err != nil {
        fmt.Println("Error sending acknowledgment:", err)
    }
}

// ------------- Juice Handler ---------------

func startJuiceServer() {
    HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", HostName+juicePort)
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
			go handleJuiceTask(conn)
		} 
	}
}

func handleJuiceTask(conn net.Conn) {
    defer conn.Close()

    var task JuiceTask
    decoder := json.NewDecoder(conn)
    err := decoder.Decode(&task)
    if err != nil {
        fmt.Println("Error decoding enhanced tasks:", err)
        return
    }

    fmt.Printf("Received task: %+v\n", task)

    // Fetch JuiceExe and all assigned filenames to local
    handleSendingFileCommand("get " + task.JuiceExe)

    totalResult := ""
    // Execute command for each filename
    for _, filename := range task.Filenames {
        handleSendingFileCommand("get " + filename)

        additionalArgs := []string{"run", task.JuiceExe, filename}
        additionalArgs = append(additionalArgs, task.Parameters...)

        fmt.Println(additionalArgs)

        // Construct the command to execute maple.go
        cmd := exec.Command("go", additionalArgs...)
        output, err := cmd.CombinedOutput()
        fmt.Println(string(output))
        if err != nil {
            fmt.Printf("Error running command for file %s: %s\n", filename, err)
            fmt.Fprintln(conn, "Failed\n") // Send error message over connection
            return
        }
        fmt.Println(string(output))
        if task.OutputMode == "" || task.OutputMode == "0" {
            totalResult += (ExtractKey(filename, task.SdfsIntermediateFilename) + " : " + string(output))
        } else {
            totalResult += string(output)
        }

        // Delete the file at the end
        err = os.Remove(filename)
        if err != nil {
            fmt.Println("Error deleting file:", err)
            return
        }
    }
    fmt.Println(totalResult)

    // write totalResult to local and append to storage
    file, err := os.Create(task.SdfsDestFilename)
    if err != nil {
        fmt.Println("Error creating file:", err)
        return
    }
    defer file.Close()

    _, err = io.WriteString(file, totalResult)
    if err != nil {
        fmt.Println("Error writing to file:", err)
        return
    }

    handleSendingFileCommand("append " + task.SdfsDestFilename)

    // Delete the file at the end
    err = os.Remove(task.SdfsDestFilename)
    if err != nil {
        fmt.Println("Error deleting file:", err)
        return
    }

    // Write ACK message if no errors occurred
    fmt.Fprintln(conn, "ACK\n")
}
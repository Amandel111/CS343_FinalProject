package main

import (
    "bufio"
    "fmt"
    "log"
    "math/rand"
    "net/http"
    "net/rpc"
    "os"
    "strconv"
    "sync"
    "time"
    "math"
    "path/filepath"
    "io/ioutil"
)

type VoteArguments struct {
    Term        int
    CandidateID int
    LastLogIndex int
    LastLogTerm int
}

type VoteReply struct {
    Term       int
    ResultVote bool
}

// What are these two structs
type AppendEntryArgument struct {
    Term         int
    LeaderID     int
    Entries      LogEntry
    PrevLogEntry LogEntry
    PrevLogIndex int
    PrevLogTerm  int
    LeaderCommit int
    CommandType  string //"R", "W"
    FileName     string // created in ClientAddToLog function
    
}

type AppendEntryReply struct {
    Term    int
    Success bool
}

type ServerConnection struct {
    serverID      int
    Address       string
    rpcConnection *rpc.Client
}

type LogEntry struct {
    Index int
    Term  int
}

type RaftNode struct {
    selfID          int
    leaderID        int
    serverNodes     []ServerConnection
    currentTerm     int
    votedFor        int
    state           string // "follower", "candidate", "leader"
    Mutex           sync.Mutex
    electionTimeout *time.Timer
    commitIndex     int
    log             []LogEntry
    nextIndex       map[int]int // nodeID: nodeIndex
    matchIndex      map[int]int //use this for leader to know when majority of servers have replicated an entry
    lastApplied     int
}

type ClientArguments struct {
    EntityID string 
    EntityType string // i.e. "post", "user"
    CommandType string // "R" for read, "W" for write
    Data string // empty for reads; data that client wants to write
}

type ClientReply struct {
    Content string  // if "R" return this content to client, elif "W" return empty string for content
//  FileName string // we make as "EntityType + EntityID"
    Success bool // true only after data is applied to file and all logs are consistent
    //LeaderID int
    LeaderAddress string
}

// Helper Functions -------------------------
// This file adds the file (entityType + entityID) to the directory
// called in readFile() before reading or writing to a file 
// if file or dir doesn't exists then this function is called
func createDirAndFile(fileName string, dirName string) error {
    fmt.Println("Calling CREATEDIRANDFILE")

    // Check if directory already exists
    if _, err := os.Stat(dirName); os.IsNotExist(err) {
        // Directory does not exist, so create it
        err := os.Mkdir(dirName, 0777)
        if err != nil {
            fmt.Println("Error creating directory:", err)
            return err
        }
        fmt.Println("Directory created successfully:", dirName)
    } else if err != nil {
        // Some other error occurred while checking directory existence
        fmt.Println("Error checking directory existence:", err)
        return err
    } else {
        // Directory already exists
        fmt.Println("Directory already exists:", dirName)
    }

    // Creating the file path within the directory
    filePath := filepath.Join(dirName, fileName)

    // Create the file
    file, err := os.Create(filePath)
    if err != nil {
        fmt.Println("Error creating file:", err)
        return err
    }
    defer file.Close()
    fmt.Println("File created successfully:", filePath)

    return nil
}

// This function locates the file and overwrites it with the new data that the client specifies
// data: from ClientArguments Data variable passed in from ClientAddToLog() when writeFile() is called
func writeFile(fileName string, dirName string, data string) error {
    fmt.Println("Calling WRITEFILE")

    // Constructing the file path within the directory
    filePath := filepath.Join(dirName, fileName)

    // Check if file exists
    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        // File does not exist, create it
        createFileErr := createDirAndFile(fileName, dirName)
        if createFileErr != nil {
            fmt.Println("Error creating file in writeFile:", createFileErr)
            return createFileErr
        }
    }

    // Open the file for writing
    file, openFileErr := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0644)
    if openFileErr != nil {
        fmt.Println("Error opening file in writeFile:", openFileErr)
        return openFileErr
    }
    defer file.Close()

    // Write data to the file
    _, writeDataErr := file.WriteString(data)
    if writeDataErr != nil {
        fmt.Println("Error writing data to file in writeFile:", writeDataErr)
        return writeDataErr
    }

    fmt.Println("Data written successfully to file:", filePath)
    return nil
}


func readFile(fileName string, dirName string) (string, error) {
    fmt.Println("Calling READFILE")
    filePath := filepath.Join(dirName, fileName)
    // Check if file exists
    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        fmt.Println("file does not exist in dirName:", dirName)
        fmt.Println("file name that we couldn't find in readFile: ", fileName)
        return "", err // error should not be nil since file did not exist
    }

    // Open the file in read mode
    file, openFileErr := os.Open(filePath)
    if openFileErr != nil {
        fmt.Println("Error opening file in readFile:", openFileErr)
        return "", openFileErr
    }
    defer file.Close()

    // Read all contents of the file
    content, readErr := ioutil.ReadAll(file)
    if readErr != nil {
        fmt.Println("Error reading file in readFile:", readErr)
        return "", readErr
    }

    // Convert the content to a string
    contentStr := string(content)
    fmt.Println("Successfully read file:", filePath)
    return contentStr, nil
}

// This function is designed to emulate a client reaching out to the
// server. Note that many of the realistic details are removed, for simplicity
func (node *RaftNode) ClientAddToLog(args ClientArguments, clientReply *ClientReply) error {
    fmt.Print("clientAddToLog called on node ", args)
    dirName := "CS343"
        node.Mutex.Lock()
        if node.state == "leader" {

            // make filename for the file the client is requesting to read/write to
            fileName := args.EntityType + args.EntityID
            fmt.Println("fileName for client request: ", fileName)

            // handle client requests
            if args.CommandType == "R" {
                file, err := readFile(fileName, dirName)
                if err != nil{
                    clientReply.Success = false
                    fmt.Println("Error in readFile in ClientAddToLog")
                    return err
                }
                clientReply.Success = true
                clientReply.Content = file
            } else if args.CommandType == "W" {
                err := writeFile(fileName, dirName, args.Data)
                if err != nil {
                    clientReply.Success = false
                    fmt.Println("Error in writeFile in ClientAddToLog")
                    return err
                }
                clientReply.Content = "" // empty reply content
                clientReply.Success = true
            }
            
            entry := LogEntry{len(node.log), node.currentTerm}
            log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))

            // leader: add entry to log
            node.log = append(node.log, entry)
            fmt.Println("leader's log after appending: ", node.log);
            
            //make local copy of currentTerm for safety outside lock
            var tempCurrentTerm int
            tempCurrentTerm = node.currentTerm 
    
            node.Mutex.Unlock()

            for _, peer := range node.serverNodes {

                followerPrevLogIndex := node.nextIndex[peer.serverID] - 1
                fmt.Println("follower prev log index ", followerPrevLogIndex)
                fmt.Println("leader entry ", node.log[followerPrevLogIndex+1])

                var prevLogEntry LogEntry
                fmt.Println("checking 207")
                //if log list has more than one element
                if len(node.log) > 1 {
                    fmt.Println("checking followerPrevLogIndex: ", followerPrevLogIndex)
                    prevLogEntry = node.log[followerPrevLogIndex]
                    //prevLogEntry = node.log[followerPrevLogIndex+1]
                } else {
                    fmt.Println("checking prevLogEntry before: ", prevLogEntry)
                    //this case can be used to check if leader log is just starting, don't actually append this
                    prevLogEntry = LogEntry{-1, -1}
                    fmt.Println("checking prevLogEntry after: ", prevLogEntry)
                }

                fmt.Println("leader prev entry ", prevLogEntry)

                if len(node.log)-1 >= node.nextIndex[peer.serverID] { //follower is not up to date
                    // Construct arguments for AppendEntry RPC call
                    args := AppendEntryArgument{
                        Term:         tempCurrentTerm, //node.currentTerm, temp var for safety
                        LeaderID:     node.selfID,
                        Entries:      node.log[followerPrevLogIndex+1],
                        PrevLogEntry: prevLogEntry,
                        LeaderCommit: node.commitIndex,
                    }

                    // Create a reply variable to store the response
                    var reply AppendEntryReply

                    // Call AppendEntry RPC on the peer
                    var wg sync.WaitGroup
                    err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
                    if err != nil {
                        fmt.Printf("Error calling AppendEntry in Client to node %d: %v\n", peer.serverID, err)
                        // Handle the error appropriately, e.g., mark the peer as unreachable
                    } else {
                        for !reply.Success {
                            //if the rpc failed, two cases: either this is not the leader node, or follower node's log is inconsistent
                            
                            fmt.Println("log entry appending failed for node ", peer.serverID, "try again")
                            
                            if node.currentTerm < reply.Term{
                                node.Mutex.Lock()
                                //leader is out of date
                                fmt.Println("leader out of date")
                                node.transitionToFollower()
                                node.votedFor = -1
                                node.currentTerm = reply.Term
                                node.resetElectionTimeout()
                                node.Mutex.Unlock()
                                // TODO: Success is false, Content is empty
                                // clientReply := ClientArguments
                                return nil 
                            } else {
                                go func() {
                                    wg.Add(1)
                                    defer wg.Done()
                                    //the follower node's log is inconsistent, decrement nextIndex
                                    node.nextIndex[peer.serverID] -= 1
                                    //node.nextIndex[peer.serverID] =int(math.Max(float64(node.nextIndex[peer.serverID] - 1), float64(0))) // dubious edit!

                                    //set argument up
                                    followerPrevLogIndex := node.nextIndex[peer.serverID] - 1
                                    fmt.Println("retry follower prev log index ", followerPrevLogIndex)
                                    fmt.Println("retry leader entry ", node.log[followerPrevLogIndex+1])
                    
                                    var prevLogEntry LogEntry
                                    //if log list has more than one element
                                    if len(node.log) > 1 {
                                        prevLogEntry = node.log[followerPrevLogIndex]
                                    } else{
                                        //this case can be used to check if leader log is just starting, don't actually append this
                                        prevLogEntry = LogEntry{-1, -1}
                                    }
                                        
                                    // Construct arguments for AppendEntry RPC call
                                    args := AppendEntryArgument{
                                        Term:         tempCurrentTerm,
                                        LeaderID:     node.selfID,
                                        Entries:      node.log[followerPrevLogIndex+1],
                                        PrevLogEntry: prevLogEntry,//node.log[followerPrevLogIndex], //prevLogEntry, //fix this so that second argument makes sense
                                        LeaderCommit: node.commitIndex,
                                    }

                                    err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
                                    fmt.Print("retyr called rpc in ClientCall");
                                    if err != nil {
                                        fmt.Printf("Error calling AppendEntry in Client to node %d: %v\n", peer.serverID, err)
                                    }
                                }()
                                wg.Wait()           
                            }
                        }
                        //append worked, increment nextIndex
                        fmt.Print("append entry returned success")
                        node.Mutex.Lock()
                        node.nextIndex[peer.serverID] += 1
                        fmt.Println("node.nextIndex for ", peer.serverID, " is ", node.nextIndex[peer.serverID])
                        node.matchIndex[peer.serverID] += 1
                        node.Mutex.Unlock()
                    }
                }
            }
            
            //count number of followers that have replicated
            totalReplicated := 0 

            fmt.Println("Initialized totalReplicated")
            node.Mutex.Lock()
            if (len(node.log) - 1 > node.commitIndex ){
                fmt.Printf("there is an uncommitted log")
                // there is an uncomitted entry
                for _, replicatedIndex := range node.matchIndex {
                    if (replicatedIndex >= len(node.log) -1) && node.log[len(node.log) - 1].Term == tempCurrentTerm{//node.currentTerm{
                        totalReplicated++
                    }
                }
            }

            //if majority followers have replicated
            if totalReplicated > len(node.serverNodes)/2 {
                fmt.Printf("Leader got majority replicated logs")
                node.commitIndex = len(node.log) - 1
            }
            node.Mutex.Unlock()
        } else {
            node.Mutex.Unlock()
            fmt.Println("This node is not the leader, don't call clientCall")
            clientReply.Success = false
            //clientReply.LeaderID = node.leaderID

            //to return the address of the leader node:
            clientReply.LeaderAddress = node.serverNodes[node.leaderID].Address
            
        }
        //time.Sleep(40 * time.Millisecond) //40
        return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
    // check the term that is coming in
    node.Mutex.Lock()
    defer node.Mutex.Unlock()

    // Check if the leader's term is less than receiving
    if arguments.Term < node.currentTerm {

        // Reply false if leader's term is older
        fmt.Println("leader's term is outdated")
        reply.Term = node.currentTerm
        reply.Success = false

    } else {
        //fmt.Print("correct leader in append argument RPC")
        
            // Update term to match the leader's term and transition to follower state
            node.currentTerm = arguments.Term

        if (arguments.Entries.Term == -1){

            //this is the heartbeat, we use fake term -1 to distinguish btwn heartbead and entry appends
            
            // Reset the election timeout as the leader is now active, this node is def not leader so step down
            node.resetElectionTimeout()
            node.transitionToFollower()
            node.leaderID = arguments.LeaderID // let follower know who the leader is
        } else {
            fmt.Println("arguments to AppendEntry for node ", node.selfID, " : ", arguments)
            fmt.Println("in term ", arguments.Term, "passed entry: ", arguments.Entries)

            // Reply to the leader with success
            reply.Term = node.currentTerm

            //base case, list is empty
            if len(node.log) == 0 {
                //empty log, append entries
                if (arguments.PrevLogEntry.Term) == -1{
                    //this is the case where the leader just started new log
                    fmt.Println("Log is empty")
                    node.log = append(node.log, arguments.Entries)
                    node.commitIndex = int(math.Min(float64(arguments.Entries.Index), float64(arguments.LeaderCommit)))
                    reply.Success = true
                } else{
                    // leader has more than one entry so follower is behind
                    reply.Success = false
                }
            } else if (len(node.log) - 1) < arguments.PrevLogEntry.Index {
                //this is if the log doesn't contain an entry at prevLogIndex
                reply.Success = false
                fmt.Print("the follower log does not contain an entry at prevLogIndex ", arguments.PrevLogEntry.Index)
            } else if (len(node.log) - 1) > arguments.PrevLogEntry.Index { //NOTE: swap this so check for terms FIRST
                //the follower's log has too many entries
                fmt.Println("LOG IS TOO LONG FOR NODE ", node.selfID)
                fmt.Println("arguments.PrevLogEntry.Index", arguments.PrevLogEntry.Index);
                //delete any logs that are at a higher index that prevLogIndex,
                //node.log = node.log[0:node.log[prevLogIndex -1]] //delete that entry and all that follow
                if node.log[arguments.PrevLogEntry.Index].Term != arguments.PrevLogEntry.Term {
                    //node.log = node.log[0 : arguments.PrevLogIndex-1] //delete that entry and all that follow
                    fmt.Print("previous index is wrong in this long log")
                    node.log = node.log[0 : len(node.log) - 1]
                    reply.Success = false
                }
            } else if (len(node.log) - 1) == arguments.PrevLogEntry.Index { //length of node log is at least as up to date as leader's
                fmt.Println("LOG IS CORRECT LENGTH FOR NODE ", node.selfID)
                fmt.Println("node's prev term ",  node.log[arguments.PrevLogEntry.Index].Term , " and leader's prevLogTerm ", arguments.PrevLogTerm, "and node's log ", node.log)
                if node.log[arguments.PrevLogEntry.Index].Term != arguments.PrevLogEntry.Term {
                    fmt.Println("LOG HAS WRONG TERM ENTRY FOR NODE ", node.selfID)
                    //node.log = node.log[0 : arguments.PrevLogIndex-1] //#delete that entry and all that follow //index error here
                    node.log = node.log[0 : len(node.log) - 1]
                    reply.Success = false
                } else {
                    //append new entries in log
                    fmt.Println("APPENDING SUCCEEDS FOR NODE ", node.selfID)
                    node.log = append(node.log, arguments.Entries)
                    node.commitIndex = int(math.Min(float64(arguments.Entries.Index), float64(arguments.LeaderCommit)))
                    reply.Success = true
                }
            }
    }
}
    return nil
}


// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
    // Locking to prevent race conditions
    node.Mutex.Lock()
    defer node.Mutex.Unlock()
    fmt.Printf("Node %d received vote request from Node %d with term %d\n", node.selfID, arguments.CandidateID, arguments.Term)

    //fmt.Println("receiving node's log: ", node.log)

    receiverLastLogTerm := -1
    receiverLastLogIndex := -1
    if (len(node.log)-1 > 0){
        receiverLastLogTerm = node.log[len(node.log)-1].Term
        receiverLastLogIndex = len(node.log)-1
    }
    // Check that the term of the requester is higher
    if arguments.Term > node.currentTerm && (arguments.LastLogIndex >= receiverLastLogIndex) && (arguments.LastLogTerm >= receiverLastLogTerm){
        node.currentTerm = arguments.Term // receiver node will update current term to match canddiate's term
        node.votedFor = -1                // reset this count since candidate's term is larger
        // Acknowledging vote or not to the candidate
        reply.Term = node.currentTerm
        reply.ResultVote = true               // receiver node will vote yes
        node.votedFor = arguments.CandidateID // Vote for the candidate
        fmt.Printf("Node %d voted for Node %d in term %d\n", node.selfID, arguments.CandidateID, arguments.Term)

    } else {
        fmt.Println(node.selfID, " at term ", node.currentTerm, " rejected vote")
        reply.Term = node.currentTerm
        reply.ResultVote = false // Vote not granted
    }
    return nil
}

// resetting the election timeout to a random duration
func (node *RaftNode) resetElectionTimeout() {
    //fmt.Println("reset timer for node ", node.selfID)
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    duration := time.Duration(r.Intn(150)+150) * time.Millisecond
    node.electionTimeout.Stop()
    node.electionTimeout.Reset(duration)
}

func (node *RaftNode) transitionToFollower() {
    //fmt.Println("node ", node.selfID, "transitions to follwoer")
    node.state = "follower"
}

func (node *RaftNode) transitionToCandidate() {
    node.state = "candidate"
    node.votedFor = node.selfID
}

func (node *RaftNode) transitionToLeader() {
    fmt.Println("node ", node.selfID, "transitions to leader")
    node.state = "leader"
    node.resetElectionTimeout() // Reset the election timeout since the node is now leader

    for _, peer := range node.serverNodes {
        node.nextIndex[int(peer.serverID)] = len(node.log) //+ 1
        node.matchIndex[peer.serverID] = 0
    }
    go Heartbeat(node, node.serverNodes)
    //go node.ClientAddToLog()
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func (node *RaftNode) LeaderElection() {
    node.transitionToCandidate()
    // not do anything until timer timesout
    fmt.Printf("Node %d starts leader election\n", node.selfID)
    node.Mutex.Lock()
    node.currentTerm++
    node.votedFor = node.selfID
    node.Mutex.Unlock()

    fmt.Print("candidate log: ", node.log)
    fmt.Print("candidate log length: ", len(node.log))
    lastLogIndex := -1
    lastLogTerm := -1
    if len(node.log)-1 > 0 {
        fmt.Println("THE LENGTH OF THE LOG IS NOT ZERO")
        lastLogIndex = len(node.log) - 1
        lastLogTerm = node.log[len(node.log) - 1].Term
    }

    // node stops receiving heartbeats, notices something is wrong
    arguments := VoteArguments{
        Term:        node.currentTerm,
        CandidateID: node.selfID,
        LastLogIndex: lastLogIndex,
        LastLogTerm: lastLogTerm,
    }
    var reply VoteReply
    // Count of received votes
    votesReceived := 1

    // sending nil as vote, but how do we handle that, what do I pass?
    fmt.Printf("Node %d requests votes for term %d\n", node.selfID, arguments.Term)

    var wg sync.WaitGroup
    // Start vote requests to other nodes
    for _, server := range node.serverNodes {
        wg.Add(1)
        go func(server ServerConnection) {
            defer wg.Done()
            err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
            if err != nil {
                fmt.Printf("Error sending vote request to node %d: %v\n", server.serverID, err)
                return
            }
            if reply.Term > node.currentTerm {
                node.Mutex.Lock()
                node.currentTerm = reply.Term
                node.votedFor = -1
                node.Mutex.Unlock()
                return
            }
            if reply.Term == node.currentTerm && reply.ResultVote {
                node.Mutex.Lock()
                votesReceived++
                node.Mutex.Unlock()
            }
        }(server)
    }
    wg.Wait()

    if votesReceived > len(node.serverNodes)/2 {
        node.Mutex.Lock()
        fmt.Printf("Node %d becomes leader for term %d\n", node.selfID, node.currentTerm)
        node.Mutex.Unlock()
        node.transitionToLeader()
    } else {
        fmt.Printf("Node %d failed to become leader for term %d\n", node.selfID, node.currentTerm)
        node.transitionToFollower()
    }

}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(node *RaftNode, peers []ServerConnection) {
    for { // infinite loop
        // Lock the node's state for consistency
        node.Mutex.Lock()

        // Check if the node is the leader
        if node.state == "leader" && node.votedFor == node.selfID {
            // Unlock the node's state before sending heartbeats
            node.Mutex.Unlock()

            // If the node is the leader, it'll send a heartbeat message to all other nodes
            for _, peer := range peers {
                //if peer.serverID != node.selfID {
                // Construct arguments for AppendEntry RPC call
                args := AppendEntryArgument{
                    Term:         node.currentTerm,
                    LeaderID:     node.selfID,
                    Entries:      LogEntry{-1, -1},
                    PrevLogEntry: LogEntry{-1, -1},
                    LeaderCommit: 0,
                }

                // Create a reply variable to store the response
                var reply AppendEntryReply

                // Call AppendEntry RPC on the peer
                err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
                if err != nil {
                    fmt.Printf("Error sending heartbeat to node %d: %v\n", peer.serverID, err)
                    // Handle the error appropriately, e.g., mark the peer as unreachable
                } else {
                    //fmt.Printf("Sent heartbeat to node %d\n", peer.Address)
                }
                //}
            }
        } else {
            // If the node is no longer the leader, stop sending heartbeats
            node.Mutex.Unlock()
            return
        }
        
        time.Sleep(100 * time.Millisecond)
    }
}

func main() {
    // The assumption here is that the command line arguments will contain:
    // This server's ID (zero-based), location and name of the cluster configuration file
    arguments := os.Args
    if len(arguments) == 1 {
        fmt.Println("Please provide cluster information.")
        return
    }

    // Get this sever's ID (same as its index for simplicity)
    myID, err := strconv.Atoi(arguments[1])
    // Get the information of the cluster configuration file containing information on other servers
    file, err := os.Open(arguments[2])
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    myPort := "localhost"

    // Read the IP:port info from the cluster configuration file
    scanner := bufio.NewScanner(file)
    lines := make([]string, 0)
    index := 0
    for scanner.Scan() {
        // Get server IP:port
        text := scanner.Text()
        log.Printf(text, index)
        if index == myID {
            myPort = text
            index++
            continue
        }
        // Save that information as a string for now
        lines = append(lines, text)
        index++
    }
    // If anything wrong happens with readin the file, simply exit
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

    // initialize parameters
    node := &RaftNode{
        selfID:      myID,
        leaderID:    -1, // initially -1 
        currentTerm: 0,
        state:       "follower",
        votedFor:    -1,
        Mutex:       sync.Mutex{},
        commitIndex: 0,
        log:         make([]LogEntry, 0),
        nextIndex:   make(map[int]int),
        matchIndex:  make(map[int]int),
    }

    // Following lines are to register the RPCs of this object of type RaftNode
    //api := new(RaftNode)
    err = rpc.Register(node)
    if err != nil {
        log.Fatal("error registering the RPCs", err)
    }
    rpc.HandleHTTP()
    go http.ListenAndServe(myPort, nil)
    log.Printf("serving rpc on port" + myPort)

    for index, element := range lines {
        // Attemp to connect to the other server node
        client, err := rpc.DialHTTP("tcp", element)
        // If connection is not established
        for err != nil {
            // log.Println("Trying again. Connection error: ", err)
            // Try again!
            client, err = rpc.DialHTTP("tcp", element)
        }
        // Once connection is finally established
        // Save that connection information in the servers list
        node.serverNodes = append(node.serverNodes, ServerConnection{index, element, client})
        // Record that in log
        fmt.Println("Connected to " + element)
    }

    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
    node.electionTimeout = time.NewTimer(tRandom)

    go func() {
        fmt.Printf("Remaining time: %v\n", node.electionTimeout)
        <-node.electionTimeout.C

        fmt.Println("start leader election from main timeout")
        node.LeaderElection()
    }()
    //go node.ClientAddToLog()

    // TODO: put this in another process (another file) and make sure
    // ClientAddToLog returns somethings that let's the client file
    // know if the node it chose was the leader or not so we know
    // to re-run the RPC call in the client file
    /*go func() { 
        time.Sleep(300 * time.Millisecond)
        for _, server := range node.serverNodes {
        clientArgs := ClientArguments{
            EntityID: 1,
            EntityType: "user",
            CommandType: "W",
            Data: "testing",
        }
        var clientReply ClientReply
    
        err := server.rpcConnection.Call("RaftNode.ClientAddToLog", clientArgs, &clientReply)
        if err != nil {
            fmt.Printf("Error callng ClientAddToLog: %v\n", err)
            return
        }
    }
    return 
    }()*/
    var wg sync.WaitGroup
    wg.Add(1)
    wg.Wait()
    // ** Once leader election ends, then check if 'I am the leader'
    // ** If node is leader, then call heartbeats all the time

}

/*
1. client reaches out to servers
2. if server is leader, it will append that entry to its log
3. it will send that entry via AppendEntry to all other servers
    Case 1: Missing entries
    a. for each server, if server's last term is outdated, it fails, and leader will decrement
    next index and try again until they find a match.
    Case 2: Wrong entries (mismatch)
    a. for each server, if server's last entry is diff but at the same index as leader's, then delete,
    delete entry and all that follow until a match is found
    b. Delete any entries after that point and replace with the
    append any new entries not already in the log, until the server's last log index == leader's commitIndex
    update node's committedIndex every appendEntry to be either leader's commitIndex or index of last new enty
4. If majority of the servers replicate it then leader commits entry, notifies servers it has committed

*/

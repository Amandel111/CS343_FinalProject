package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"
)

type ClientArguments struct {
	EntityID int 
	EntityType string // i.e. "post", "user"
	CommandType string // "R" for read, "W" for write
	Data string // empty for reads; data that client wants to write
}

type ClientReply struct {
	Content string  // if "R" return this content to client, elif "W" return empty string for content
//	FileName string // we make as "EntityType + EntityID"
	Success bool // true only after data is applied to file and all logs are consistent
	LeaderID int // to let client know who the current leader is to try the client call to the current leader
}

type ServerConnection struct {
	serverID      int
	Address       string
	//rpcConnection *rpc.Client
}


func main() { // The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[1])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	
	//randomly choose a server
	serverNodes := make([]ServerConnection, 0)
	for index, element := range lines {
		serverNodes = append(serverNodes, ServerConnection{index, element /*clients*/})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	//client add to log
	time.Sleep(300 * time.Millisecond)
	clientArgs := ClientArguments{
		EntityID: 3,
		EntityType: "user",
		CommandType: "W",
		Data: "",
	}
	var clientReply ClientReply
	// If connection is not established
	client, err := rpc.DialHTTP("tcp", serverNodes[0].Address)
	// If connection is not established
	for err != nil {
		// log.Println("Trying again. Connection error: ", err)
		// Try again!
		client, err = rpc.DialHTTP("tcp", serverNodes[0].Address)
	}

	err = client.Call("RaftNode.ClientAddToLog", clientArgs, &clientReply)
	if err != nil {
		fmt.Printf("Error callng ClientAddToLog: %v\n", err)
		return
	}

	// if clientaddtolog was called on a node that wasn't a leader, try again with the current leader
	if !clientReply.Success {
		client, err := rpc.DialHTTP("tcp", serverNodes[clientReply.LeaderID].Address)
		// If connection is not established
		for err != nil {
			// log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", serverNodes[clientReply.LeaderID].Address)
		}

		err = client.Call("RaftNode.ClientAddToLog", clientArgs, &clientReply)
		if err != nil {
			fmt.Printf("Error callng ClientAddToLog: %v\n", err)
			return
		}
	} else{
		fmt.Println("data: ", clientReply.Content)
	}
}
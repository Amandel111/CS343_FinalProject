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
)

//type RaftNode int

type RaftNode struct {
	mutex            sync.Mutex
	selfID           int
	myPort           string
	currentTerm      int
	peerConnections  []ServerConnection
	electionTimeout  *time.Timer
	status           string
	votedFor         int
	voteCount        int
	log              []LogEntry
	commitIndex      int
	lastAppliedIndex int
	nextIndex        []int // array for leader to keep track of which index the append entry to a follower's log
}

type VoteArguments struct {
	Term         int
	CandidateID  int
	lastLogIndex int // candidate's last log entry
	lastLogTerm  int // candidate's last log term
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // maybe not need since not using commit index?
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

var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var electionTimeout *time.Timer

const NUM_NODES = 5

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Println(node.selfID, "recieved a vote request from", arguments.CandidateID)
	//node.resetElectionTimeout()                                    //do we do this before or after we compare terms
	if arguments.Term > node.currentTerm { //|| (arguments.Term == node.currentTerm && node.votedFor == -1) {// && node.votedFor == -1 { //reset votedFor at beginning of  //=> //do we get rid of node.votedFor?
		fmt.Println("candidate has greater term, vote for candidate", arguments.CandidateID)
		node.resetElectionTimeout()
		// QUESTION: how can a node vote for 2 candidates (if the higher term one is the second candidate)
		//step down
		//node.votedFor = -1
		//increment node term
		//node.currentTerm = arguments.Term

		//candidate has valid term numver, approve vote
		node.votedFor = arguments.CandidateID
		node.status = "follower"
		fmt.Println("node ", node.selfID, " votes for a candidate other than itself")
		reply.ResultVote = true
		fmt.Println("node ", node.selfID, " votes for node ", node.votedFor)

	} else if arguments.Term == node.currentTerm && node.votedFor == -1 {
		fmt.Println("terms equal, node ", node.selfID, " vote for candidate ", arguments.CandidateID)
		node.resetElectionTimeout()
		//node.currentTerm = arguments.Term
		node.status = "follower"
		//candidate has valid term numver, approve vote
		fmt.Println("node votes for a candidate other than itself")
		node.votedFor = arguments.CandidateID
		reply.ResultVote = true
	} else { //ask christine if less than or equal to or just less than
		//candidate has equal to or smaller term number, invalid
		reply.ResultVote = false
	}

	reply.Term = node.currentTerm

	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (followerNode *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	fmt.Println("followernode", followerNode.log)
	// if leader's term is less than follower node's term
	if arguments.Term < followerNode.currentTerm {
		fmt.Println("leader term less than follower node term")
		reply.Success = false
		reply.Term = followerNode.currentTerm // so that reply has most updated term
		return nil
	} else { // no other issues so update follower node's info so they know who the leader is
		//followerNode.currLeaderID = arguments.LeaderID
		// NOTE: handles case where dead leader comes back to life and recognized that it's not the leader anymore
		followerNode.status = "follower"
		// followers receiving heartbeats from leader so it resets its election timer since it knows leader is alive
		followerNode.resetElectionTimeout()
	}

	// in order to reply with a success for appendentry we need to make sure follower's log is consistent with leader's log
	// check if its a heartbeat
	if arguments.PrevLogIndex != -1 { // if not a heartbeat
		fmt.Println("not a heartbeat")
		fmt.Println("appending entry", arguments.Entries)
		fmt.Println("appending entry", arguments.Entries[0])
		fmt.Println("appending entry", arguments.Entries[len(arguments.Entries)-1].Index+1)
		// length of leader log is arguments.Entries[len(arguments.Entries)-1].Index +1
		if len(followerNode.log) > arguments.Entries[len(arguments.Entries)-1].Index+1 { // rn its 1 0
			fmt.Println("follower log is longer", len(followerNode.log), arguments.Entries[len(arguments.Entries)-1].Index+1)
			fmt.Println("followerNode", followerNode.log)
			fmt.Println("leader log", arguments.Entries)
			// if follower's log is longer than leader's log, delete everything from PrevLogIndex
			followerNode.log = followerNode.log[0 : arguments.Entries[len(arguments.Entries)-1].Index+1]
			// reply false bc we still need to find the latest matching entry in follower log
			reply.Success = false
			return nil
			//followerNode.log = append(followerNode.log, arguments.Entries...)
		} else if len(followerNode.log) < arguments.Entries[len(arguments.Entries)-1].Index+1 { // RN ITS 0 0
			fmt.Println("follower log is shorter than leader log", len(followerNode.log), arguments.Entries[len(arguments.Entries)-1].Index+1)
			// follower log is shorter than leader log
			// TODO: check this logic - does clientaddtolog function take care of this?
			reply.Success = false
			return nil
		}
		fmt.Println("same length")
		//fmt.Println("length is equal")
		// by now, follower's log is same length as leader's log - so check terms to see if it matches
		// NOTE: added length != 0 to prevent index out of range error
		//fmt.Println("follower Node last applied index", followerNode.log[followerNode.lastAppliedIndex].Term)
		// check not -1 because then it will be empty and can append whatever
		if followerNode.lastAppliedIndex == -1 {
			followerNode.log = append(followerNode.log, arguments.Entries...)
			followerNode.lastAppliedIndex++
		} else if followerNode.log[followerNode.lastAppliedIndex].Term != arguments.Entries[followerNode.lastAppliedIndex].Term {
			fmt.Println("terms don't match, follower log", followerNode.log[followerNode.lastAppliedIndex].Term, "leader log", arguments.Entries[followerNode.lastAppliedIndex].Term)
			// terms don't match - so in clientaddtolog, we will decrement nextIndex and try again to find
			// the most recent index of the follower's log where the term matches the leader's term at that index
			reply.Success = false
			return nil
		} else {
			// if there is match - follower is consistent so append the new entry
			// NOTE: i think we need to append everything from next index to the new entry (to handle cases
			// where we find a matching log entry in the follower that is further behind the log) unless
			// we can worry about one entry at a time?
			followerNode.log = append(followerNode.log, arguments.Entries...)
			followerNode.lastAppliedIndex++
		}
	}
	fmt.Println("reply true")
	reply.Success = true
	return nil

}

// This function is designed to emulate a client reaching out to the
// server. Note that many of the realistic details are removed, for
// simplicity
func (leaderNode *RaftNode) ClientAddToLog() {
	// In a realistic scenario, the client will find the leader node and
	// communicate with it
	// In this implementation, we are pretending that the client reached
	// out to the server somehow
	// But any new log entries will not be created unless the server /
	// node is a leader
	// isLeader here is a boolean to indicate whether the node is a leader
	// or not
	successfulAppendsCount := 0 // to check for majority appends for an entry to update leader's commit index
	if leaderNode.status == "leader" {
		// lastAppliedIndex here is an int variable that is needed by a node
		// to store the value of the last index it used in the log
		var entries []LogEntry
		entry := LogEntry{Index: leaderNode.lastAppliedIndex, Term: currentTerm}
		entries = append(entries, entry)
		log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
		// NOTE FOR LATER: why do we need to entries as an array? is it possible its creating duplicates? since every add to client will just be one entry>?
		// Add rest of logic here
		// HINT 1: using the AppendEntry RPC might happen here
		for _, entry := range entries {
			leaderNode.log = append(leaderNode.log, entry)
		}
		leaderNode.log = append(leaderNode.log, entry)
		leaderNode.lastAppliedIndex++
		log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
		// prepare AppendEntry RPC arguments for the follower nodes
		args := AppendEntryArgument{Term: leaderNode.currentTerm, LeaderID: leaderNode.selfID, PrevLogIndex: leaderNode.lastAppliedIndex, PrevLogTerm: leaderNode.log[leaderNode.lastAppliedIndex].Term, Entries: entries}

		var reply AppendEntryReply
		for _, server := range leaderNode.peerConnections {
			fmt.Println("sending to", server.Address)
			err := server.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
			go func() {
				for reply.Success == false {
					fmt.Println("reply false")
					// reset entries for follower
					entries = entries[len(entries)-1:]
					fmt.Println("entries", entries)
					// grab correct indexes and entries to send out here
					//args = AppendEntryArgument{Term: leaderNode.currentTerm, LeaderID: leaderNode.selfID, PrevLogIndex: leaderNode.lastAppliedIndex, PrevLogTerm: leaderNode.log[leaderNode.lastAppliedIndex].Term, Entries: entries}
					err = server.rpcConnection.Call("RaftNode.AppendEntry", args, &reply) // follower node's append entry
					if err != nil {
						fmt.Println("Error appending entry to node with address: ", server.Address)
					}
					if reply.Term > leaderNode.currentTerm { // reply was false because leader was not up to date
						fmt.Println("leader not up to date")
						leaderNode.status = "follower"
						leaderNode.votedFor = -1
						leaderNode.currentTerm = reply.Term
						// add a timer for leader that steps down (or see how to reset leader election timer)
						// NOTE can call here
						leaderNode.resetElectionTimeout()
						break
					}
					if reply.Success == true {
						fmt.Println("Successful AppendEntry to follower node with server address: ", server.Address)
						successfulAppendsCount++
						break // break from inside 'while' loop
					} else {
						// reply was false so follower log was shorter or the term at PrevLogIndex was not matching leader's
						// need to find the latest matching term in follower's log and append the leader's entries from that point on
						fmt.Println("leader node next index", leaderNode.nextIndex)
						leaderNode.nextIndex[server.serverID]-- // update follower's next index in the leader's array (index of array is id of follower node)
						indexToApplyInFollower := leaderNode.nextIndex[server.serverID]
						prevIndexInFollower := indexToApplyInFollower - 1 // getting the previous index to check follower's log
						olderEntry := LogEntry{Index: indexToApplyInFollower, Term: leaderNode.log[indexToApplyInFollower].Term}
						// append the new entry to the front of the existing entries array so that follower log can simply append from leader's older log entries (that its missing) to the new entry
						// Create a slice to hold the updated entries
						entries = append([]LogEntry{olderEntry}, entries...)
						//args = AppendEntryArgument{Term: leaderNode.currentTerm, LeaderID: leaderNode.selfID, PrevLogIndex: prevIndexInFollower, PrevLogTerm: leaderNode.log[prevIndexInFollower].Term, Entry: entry}
						args = AppendEntryArgument{Term: leaderNode.currentTerm, LeaderID: leaderNode.selfID, PrevLogIndex: prevIndexInFollower, PrevLogTerm: leaderNode.log[prevIndexInFollower].Term, Entries: entries}
					}
				}
			}()

		}
	}
	// HINT 2: force the thread to sleep for a good amount of time (less
	// than that of the leader election timer) and then repeat the actions above.
	// You may use an endless loop here or recursively call the function
	// HINT 3: you don’t need to add to the logic of creating new log
	// entries, just handle the replication
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func (node *RaftNode) LeaderElection() {
	fmt.Println("nominate self as candidate. node:", node.selfID)
	//increment current term and status
	node.currentTerm += 1
	node.status = "candidate"

	// vote for itself
	node.voteCount += 1
	node.votedFor = node.selfID

	// send election
	arguments := VoteArguments{
		Term:         node.currentTerm,
		CandidateID:  node.selfID,
		lastLogIndex: node.lastAppliedIndex,
		lastLogTerm:  node.currentTerm,
	}

	//wait groups
	var waitgroup sync.WaitGroup

	//potential term number to update our candidate node if it is behind
	updateTerm := node.currentTerm
	//fmt.Println("peer connections ", node.peerConnections, " for node ", node.selfID)
	for _, peerNode := range node.peerConnections {
		fmt.Println("requesting from node", peerNode)
		waitgroup.Add(1)
		go func(server ServerConnection) {
			defer waitgroup.Done()
			var reply VoteReply
			//var err string
			//var connectionError = "connection is shut down"
			err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			if err != nil {
				//nothing happens, failed
			}
			//var currErr = err.Error()
			//if connectionError == currErr {
			// 	fmt.Println("true")
			// 	return
			// }
			//for err != nil {
			//err = server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			//}
			//fmt.Println("candidate ", node.selfID, " gets response: ", reply);
			if reply.ResultVote {
				//fmt.Print("node votes yes")
				node.voteCount += 1
			} else {
				if reply.Term > updateTerm {
					updateTerm = reply.Term
				}
			}
		}(peerNode)
	}
	waitgroup.Wait()
	// move this up?? NOTE to fix
	fmt.Println("candidate ", node.selfID, " got ", node.voteCount, " votes")
	if float64(node.voteCount)/float64(NUM_NODES) > 0.5 {
		fmt.Println("confirmed leader")
		node.status = "leader"
		Heartbeat(node)
		node.ClientAddToLog()
	} else {
		node.votedFor = -1
		node.status = "follower"
		fmt.Println("not enough votes- restart election again")
		//update node term, updateTerm will be node.currentTerm unless node.currentTerm is out of date
		node.currentTerm = updateTerm
	}
	node.voteCount = 0

}

/* TODO:
reset votedFor
test with failures by making noes go to sleep randomly
*/

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(node *RaftNode) {
	fmt.Println("heartbeat called")

	//start a heartbeat timer
	//node.electionTimeout = time.NewTimer(10 * time.Millisecond)

	go func() {
		var counter = 0
		for {
			node.electionTimeout = time.NewTimer(10 * time.Millisecond)
			counter += 1
			//thread for each node checking for timeout
			<-node.electionTimeout.C

			// Printed when timer is fired
			fmt.Println("heartbeat timer fired send heartbeat", node.selfID, counter)

			//send heartbeat via appendentries
			arguments := AppendEntryArgument{
				Term:         node.currentTerm,
				LeaderID:     node.selfID,
				Entries:      make([]LogEntry, 0), // Initialize an empty slice of LogEntry,
				PrevLogIndex: -1,
				PrevLogTerm:  -1,
			}
			//var waitgroup sync.WaitGroup
			for _, peerNode := range node.peerConnections {
				//waitgroup.Add(1)
				/*go*/
				func(server ServerConnection) {
					//defer waitgroup.Done()
					var reply AppendEntryReply
					err := server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
					if err != nil {
						//return
					} else {
						if !reply.Success {
							fmt.Println("reply", reply, "port", peerNode)
							//update old leader's term
							node.votedFor = -1
							if node.currentTerm < reply.Term {
								node.currentTerm = reply.Term
							}
							node.status = "follower"
							fmt.Println("step down", node.selfID)
							return
						}
					}
					//fmt.Print("reply from append entry: ", reply);
				}(peerNode)
			}
			//waitgroup.Wait()

			//start a heartbeat timer
			//node.electionTimeout = time.NewTimer(10 * time.Millisecond)
			// UNCOMMENT THE IF STATEMENT TO TEST NODE SLEEP AND COMING BACK
			// if counter == 20 {
			// 	fmt.Println("sleep started", node.selfID)
			// 	time.Sleep(10 * time.Second)
			// 	fmt.Println("woke up")
			// }
		}
	}()

}

// will initiate a timer for the node passed to it
func StartTimer(node *RaftNode) {
	//node.mutex.Lock() //dont need to protect because it will be reset every time a node reaches out to it
	//defer node.mutex.Unlock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout = time.NewTimer(tRandom)
	//fmt.Println("Timer started")
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (node *RaftNode) resetElectionTimeout() {
	// node.mutex.Lock()
	// defer node.mutex.Unlock()

	//fmt.Println("node ", node.selfID, " reset its timer")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//fmt.Println("created new")
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	//fmt.Println("random duration")
	node.electionTimeout.Stop() // Use Reset method only on stopped or expired timers
	//fmt.Println("timeout stop")
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
	// NOTE SEGMENTATION FAULT COMING FROM TIMER
	//fmt.Println("finished reseting election timeout")
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	node := &RaftNode{
		selfID:           myID,
		mutex:            sync.Mutex{},
		currentTerm:      0,
		status:           "follower",
		votedFor:         -1,
		lastAppliedIndex: -1,
		log:              make([]LogEntry, 0), // Initialize an empty slice of LogEntry
		commitIndex:      0,
		nextIndex:        make([]int, 0),
	}
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
			node.myPort = myPort
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

	// Following lines are to register the RPCs of this object of type RaftNode
	//api := new(RaftNode)
	//err = rpc.Register(api)
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		//fmt.Println("serverNodes:", serverNodes)
		// Record that in log
		fmt.Println("Connected to " + element)
	}
	node.peerConnections = serverNodes

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

	StartTimer(node)

	go func() {
		//thread for each node checking for timeout
		<-node.electionTimeout.C

		// Printed when timer is fired
		fmt.Println("timer inactivated for node", node.selfID)

		//if node reaches this point, it starts an election because it has not received a heartbeat
		node.LeaderElection()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait() // Waits forever, so main process does not stop

}

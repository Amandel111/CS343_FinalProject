To run and test this code:
1. First open 5 terminals, one for 5 different nodes. cd into the CS343_FinalProject
2. On terminal i, run go run raftNode.go i cluster.txt where 0 <= i <= 4
3. Wait for all the nodes to connect and a leader to be elected (a print statement will show up)
4. Open a 6th terminal and run the command go run testRaft.go cluster.txt in order to request a read/write
5. To modify the specific client request, in testRaft.go navigate to lines 79-82 and provide parameters, then rerun testRaft.go

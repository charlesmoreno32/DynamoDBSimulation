package main

import (
	"CSC569lab3/shared"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
        MAX_NODES  = 8 //Update as needed
        X_TIME     = 1
        Y_TIME     = 2
        Z_TIME_MAX = 100
        Z_TIME_MIN = 10
)
var self_node shared.Node
var mu_lmemb sync.Mutex

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server *rpc.Client, id int, membership shared.Membership) {
    //TODO
    //Send uses Add to add a reqeust to the server
    success := true
    req := shared.Request{ID:id, Table:membership} //Creates new id for that node with updated membership
    (*server).Call("Requests.Add", req, &success)
}

// Read incoming messages from other nodes
func readMessages(server *rpc.Client, id int, membership shared.Membership) *shared.Membership {
    //TODO
    //read uses listen to update client from all pending requests on server
    mem := shared.NewMembership() //Creates empty membership
    err := (*server).Call("Requests.Listen", id, mem) //update membership of curr node with table of neighbor #id
    if (err != nil) {
        fmt.Println(err)
    } else {
        shared.CombineTables(&membership, mem)
    }
    return &membership
}

func leaderAnnouncement(server *rpc.Client, node *shared.Node) {
    if node.Role == shared.ROLE_LEADER {
        (*server).Call("Node.SetLeader", *node, node)
    } else {
        fmt.Println("Cannot set self as leader")
    }
}

func updateLeader(server *rpc.Client, node *shared.Node) {
    LeaderID := node.LeaderID
    err := (*server).Call("Node.GetLeader", *node, node)
    if err != nil {
        fmt.Printf("Error - updateLeader: %s\n", err)
    }
    if LeaderID != node.LeaderID {
        fmt.Printf("Leader is now node %d\n", node.LeaderID)
    }
}

func calcTime() float64 {
    //TODO
    // Gets current time
    return float64(time.Now().Unix())
}

var wg = &sync.WaitGroup{}

func main() {
        rand.Seed(time.Now().UnixNano())
        Z_TIME := rand.Intn(Z_TIME_MAX - Z_TIME_MIN) + Z_TIME_MIN

        // Connect to RPC server
        server, err := rpc.DialHTTP("tcp", "localhost:9005")

        if err != nil {
            fmt.Println("Server does not exist")
            return
        }

        args := os.Args[1:]

        // Get ID from command line argument
        if len(args) == 0 {
                fmt.Println("No args given")
                return
        }
        id, err := strconv.Atoi(args[0])
        if err != nil {
                fmt.Println("Found Error", err)
        }

        fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

        currTime := calcTime()
        // Construct self
        //leader currently starts as 1. Change later as needed.
        self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true, Term: 1,
                                Role: shared.ROLE_FOLLOWER, LeaderID: 0, Voted: false}
        if self_node.ID == 1 {
            self_node.Role = shared.ROLE_LEADER
            self_node.Term = 1
            leaderAnnouncement(server, &self_node)

        }
        var self_node_response shared.Node // Allocate space for a response to overwrite this

        // Add node with input ID
        if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
                fmt.Println("Error:2 Membership.Add()", err)
        } else {
                fmt.Printf("Success: Node created with id= %d\n", id)
        }

        neighbors := self_node.InitializeNeighbors(id)
        fmt.Println("Neighbors:", neighbors)

        membership := shared.NewMembership()
        membership.Add(self_node, &self_node)

        sendMessage(server, id, *membership)

        // crashTime := self_node.CrashTime()

        time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
        time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
//        time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

        wg.Add(1)
        wg.Wait()
}

func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
    //TODO
    // HB counter increases
    //server.Call(Node.Update??Hbcounter++)
    fmt.Println("********************* Heartbeat *********************")
    if node.Alive {
        node.Hbcounter++                //Update self
        node.Time = calcTime()

        (**membership).Update(*node, node) //update own membership

        updateLeader(server, node) //check for new leader information

        time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, membership, id) })
    }
}

func enterElection(server *rpc.Client) {
    self_node.Voted = false //mark as not voted
    self_node.LeaderID = 0 //Mark as leaderless
    self_node.Term++  //increment term
    success := false
    (*server).Call("Proposal.Clear", self_node.Term, &success)

    if success { fmt.Printf("Votes cleared from Term %d\n", self_node.Term - 1) }

    fmt.Printf("-------- Election - term %d --------\n", self_node.Term)

    // Timeout each follower has to wait before becoming candidate
    max_delay := 300
    delay := rand.Intn(150) + 150 //Get number in range 150-300
    time.Sleep(time.Duration(delay) * time.Millisecond) //Sleep random amount of time from 150 to 300

    proposal := shared.Node{}
    err := (*server).Call("Proposal.Dequeue", self_node, &proposal) //Get first proposal from term

    if err != nil {
        fmt.Printf("Error - updateLeader: %s\n", err)
    }

    if (proposal == shared.Node{}) { //No proposals yet. Proposes self as candidate
        fmt.Println("No Proposals, proposing self as leader")
        (*server).Call("Proposal.Enqueue", self_node, &self_node)
        self_node.Role = shared.ROLE_CANDIDATE
        (*server).Call("Proposal.Vote", self_node.ID, &success)
        self_node.Voted = true

        //Wait a total of 3400 ms from the beginning of the function (delay + (300 - delay) + 3100)
        time.Sleep(time.Duration(max_delay - delay + 3100) * time.Millisecond)
        count := 0
        (*server).Call("Proposal.CountVotes", self_node.ID, &count)

        if count > (MAX_NODES / 2) { //Strict majority
            self_node.Role = shared.ROLE_LEADER
            leaderAnnouncement(server, &self_node)
            self_node.LeaderID = self_node.ID
            fmt.Printf("-------- Election won by node %d with %d votes --------\n", self_node.ID, count)
            return
        } else {
            fmt.Printf("Election lost with %d votes\n", count)
            self_node.Role = shared.ROLE_FOLLOWER
            //Wait long enough for heartbeat to occur
            time.Sleep(time.Duration(1600) * time.Millisecond) //Wait for another 1500 ms to synch with followers

            if self_node.LeaderID == 0 {
                //No election has occured (updated through heartbeat)
                enterElection(server) //will loop forever if majority can't be found
            }

            fmt.Printf("Node %d successfully elected\n", self_node.LeaderID)
            return


        }
    } else {
        (*server).Call("Proposal.Vote", proposal.ID, &success)
        self_node.Voted = true //Has proposal for term, mark as voted
        fmt.Printf("Voting for proposal by node %d\n", proposal.ID)
        //Waits for a total of 1400 ms after the beginning of the function
        time.Sleep(time.Duration(max_delay - delay + 4700) * time.Millisecond) //Wait 2100-2250 ms. Total: 5000
        if self_node.LeaderID == 0 {
            //No election has occured (updated through heartbeat)
            enterElection(server) //will loop forever if majority can't be found
        }

        fmt.Printf("Node %d succsefully elected\n", self_node.LeaderID)

    }
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
    //TODO
    // Send membership to neighbors
    if self_node.Alive {
        sendMessage(server, id, **membership)
        if self_node.Role == shared.ROLE_FOLLOWER {
            mu_lmemb.Lock()
            (*membership) = readMessages(server, self_node.LeaderID, **membership)
            mu_lmemb.Unlock()
            // Do I need to check if the leader has recently updated for X time before reading the messages

            // RAFT Election: If no heartbeat from leader for Z_TIME, mark leader as dead and start election
            node, exists := (*membership).Members[self_node.LeaderID]
            if exists && !node.Alive {
                node.Role = shared.ROLE_FOLLOWER
                mu_lmemb.Lock()
                (*membership).Members[self_node.LeaderID] = node
                mu_lmemb.Unlock()
                enterElection(server)
            }


            mu_lmemb.Lock()
            (*membership) = readMessages(server, neighbors[0], **membership)
            (*membership) = readMessages(server, neighbors[1], **membership)
            mu_lmemb.Unlock()
        } else if self_node.Role == shared.ROLE_LEADER{
            for i := 1; i <= MAX_NODES; i++ {
                if i != self_node.ID {
                    mu_lmemb.Lock()
                    (*membership) = readMessages(server, i, **membership)
                    mu_lmemb.Unlock()
                }
            }
        }

        mu_lmemb.Lock()
        printMembership(**membership)
        mu_lmemb.Unlock()

        time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })
    }
}

func runAfterZ(server *rpc.Client, id int) {
    //TODO
    var node shared.Node
    (*server).Call("Membership.Get", id, &node) //update server membership table?? unnecessary perhaps. Hopefully not harmful
    node.Alive = false
    (*server).Call("Membership.Update", node, &node)
    self_node.Alive = false

    fmt.Println("NODE " + fmt.Sprintf("%d", id) + " FAILED")
    os.Exit(0)
}


func printMembership(m shared.Membership) {
    if self_node.LeaderID > 0 {
        fmt.Printf("Leader is node %d\n", self_node.LeaderID)
    } else {
        fmt.Println("NO LEADER!")
    }
    for _, val := range m.Members {
        status := "is Alive"
        if !val.Alive {
            status = "is Dead"
        }
        fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
    }
    fmt.Println("")
}

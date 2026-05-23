package shared

import (
	//    "fmt"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
    MAX_NODES      =  8 //update as needed
    Z_TIME_MIN     = 10
    ROLE_FOLLOWER  =  0
    ROLE_CANDIDATE =  1
    ROLE_LEADER    =  2
)

// Node struct represents a computing node.
type Node struct {
    ID        int
    Hbcounter int
    Time      float64
    Alive     bool
    Term      int
    Role      int //Could be enum type or constants
    LeaderID  int
    Voted     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
    rand.Seed(time.Now().Unix())
    max := 60
    min := 10
    return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
    neighbor1 := RandInt()
    for neighbor1 == id {
        neighbor1 = RandInt()
    }
    neighbor2 := RandInt()
    for neighbor1 == neighbor2 || neighbor2 == id {
        neighbor2 = RandInt()
    }
    return [2]int{neighbor1, neighbor2}
}

func (leader *Node) SetLeader(payload Node, reply *Node) error {
    if leader.Term < payload.Term && payload.Alive {
        *leader = payload
        *reply = *leader
        return nil
    }
    return errors.New("Suggested leader does not have new term")
}

func (leader *Node) GetLeader(payload Node, reply *Node) error {
    if leader.Term >= payload.Term { //should be equal as result of election
        payload.LeaderID = leader.ID
        *reply = payload
        return nil
    }
    *reply = payload
    return errors.New("No new leader")
}

func RandInt() int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Proposal struct represents a candidate proposal for RAFT election
type Proposal struct {
    Proposals map[int]Node
    Votes     map[int]int
    Term      int
    mu        sync.Mutex
}

// Returns a new instance of a Proposal (pointer).
func NewProposal() *Proposal {
    return &Proposal{
        Proposals: make(map[int]Node),
        Votes: make(map[int]int),
        Term: 1,
    }
}

// Adds a proposal to the proposals list.
func (p *Proposal) Enqueue(payload Node, reply *Node) error { //Proposals are the mailboxes
    // Go through all mailboxes and see if they have a proposal with a term less than prposal
    // If term is higher, don't update mailbox w proposal
    // If term is less or equal, add proposal to mailbox
    p.mu.Lock()
    for id := 1; id <= MAX_NODES; id++ {
        proposal, exists := p.Proposals[id]
        if !exists || (payload.Term > proposal.Term) { //Add proposal if empty or has greater term
            p.Proposals[id] = payload //First of that term, add
        }
    }
    *reply = payload
    p.mu.Unlock()
    return nil
}

// get the node proposal at the payload node's id and check if the term is equal to current term.
func (p *Proposal) Dequeue(payload Node, reply *Node) error { //Proposals are the mailboxes
    //TODO
    p.mu.Lock()
    if (p.Proposals != nil) {
        currentProposal := p.Proposals[payload.ID]
        if(currentProposal.Term == payload.Term) {
            *reply = currentProposal
            p.mu.Unlock()
            return nil
        } else {
            *reply = Node{} //empty node if not found
            p.mu.Unlock()
            return nil
        }
    }
    *reply = Node{} //If empty, return empty
    p.mu.Unlock()
    return errors.New("Proposals do not exist")
}

func (p *Proposal) Vote(ID int, reply *bool) error {
    p.mu.Lock()
    p.Votes[ID]++
    p.mu.Unlock()
    *reply = true
    return nil
}

func (p *Proposal) Clear(term int, response *bool) error {
    if (term > p.Term) { //Update term
        for vote := range p.Votes {
            delete(p.Votes, vote)
        }
        p.Term++
        *response = true
        return nil
    }
    *response = false
    return nil

}

func (p *Proposal) CountVotes(ID int, reply *int) error {
    *reply = p.Votes[ID]
    return nil
}

/*--------------*/


// Membership struct represents participanting nodes
type Membership struct {
    Members map[int]Node
    mu      sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
    return &Membership{
        Members: make(map[int]Node),
    }
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
    //TODO
    m.mu.Lock()
    if (m.Members != nil) {
        m.Members[payload.ID] = payload
        *reply = payload //May need to change HB counter/node vars
        m.mu.Unlock()
        return nil
    }
    m.mu.Unlock()
    return errors.New("Members does not exist")
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
    //TODO
    m.mu.Lock()
    m.Members[payload.ID] = payload
    m.mu.Unlock()
    *reply = payload
    return nil //errors.New("\"Update\" unimplemented")
}

func (m *Membership) Get(payload int, reply *Node) error {
    //TODO
    m.mu.Lock()
    val, exists := m.Members[payload] //Map fetches return two values!!

    if !exists { // Return error if node does not exist
        m.mu.Unlock()
        return errors.New("Node " + fmt.Sprintf("%d", payload) + " does not exist in members")
    }

    *reply = val

    m.mu.Unlock()
    return nil
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
    ID    int
    Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
    Pending map[int]Request
    mu    sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
    //TODO
    return &Requests{
        Pending: make(map[int]Request),
    }
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
    //TODO
    req.mu.Lock()
    req.Pending[payload.ID] = payload
    *reply = true // Does this have a point?
    req.mu.Unlock()
    return nil
}

// Listens to communication from neighboring node & returns table
func (req *Requests) Listen(ID int, reply *Membership) error {
    //TODO
    //Interpret ID as node listened to
    req.mu.Lock()
    request, exists := req.Pending[ID]
    if exists {
        *reply = request.Table
        req.mu.Unlock()
        return nil
    }
    req.mu.Unlock()

    return errors.New("Error: Requests.Listen() pending message from node '" + fmt.Sprintf("%d", ID) + "' does not exist")
}

func CombineTables(primary *Membership, other *Membership) *Membership {
    other.mu.Lock()   // ← lock other before reading it
    defer other.mu.Unlock()

    original := NewMembership()
    for ID, node := range primary.Members {
        original.Members[ID] = node
    }
    currTime := float64(time.Now().Unix())

    for ID, nodeO := range other.Members {
        nodeP, exists := primary.Members[ID]
        if exists {
            if nodeP.Hbcounter < nodeO.Hbcounter {
                nodeP.Hbcounter = nodeO.Hbcounter
                nodeP.Alive = true
                nodeP.Time = currTime
                primary.Members[ID] = nodeP
            }
        } else {
            nodeO.Time = currTime
            primary.Members[ID] = nodeO
        }
    }

    for ID, nodeN := range primary.Members {
        nodeO, exists := original.Members[ID]
        if exists {
            if (nodeN.Hbcounter == nodeO.Hbcounter) &&
                ((currTime - nodeO.Time) >= Z_TIME_MIN) {
                nodeN.Alive = false
                primary.Members[ID] = nodeN
            }
        }
    }
    return primary
}

type LogMessage struct {
    ToNodeID    int
    Index       int
    Term        int
    Command     string
    CommitIndex int
    PrevIndex   int
    PrevTerm    int
    LeaderID    int
}

type Log struct {
    Mailbox     map[int][]LogMessage // slice per node so nothing gets overwritten
    Entries     []LogMessage
    CommitIndex int
    mu          sync.Mutex
}

func NewLog() *Log {
    return &Log{
        Mailbox: make(map[int][]LogMessage),
        Entries: []LogMessage{},
    }
}

func (l *Log) Send(msg LogMessage, reply *bool) error {
    l.mu.Lock()
    l.Mailbox[msg.ToNodeID] = append(l.Mailbox[msg.ToNodeID], msg)
    l.mu.Unlock()
    *reply = true
    return nil
}

// returns all pending messages for this node and clears the slot
func (l *Log) Listen(nodeID int, reply *[]LogMessage) error {
    l.mu.Lock()
    defer l.mu.Unlock()
    msgs, exists := l.Mailbox[nodeID]
    if !exists || len(msgs) == 0 {
        return errors.New("no mail")
    }
    *reply = msgs
    l.Mailbox[nodeID] = []LogMessage{} // clear after reading
    return nil
}

func (l *Log) AppendEntries(msg LogMessage, reply *bool) error {
    l.mu.Lock()
    defer l.mu.Unlock()

    if msg.PrevIndex > 0 && len(l.Entries) > 0 {
        if msg.PrevIndex > len(l.Entries) ||
            l.Entries[msg.PrevIndex-1].Term != msg.PrevTerm {
            *reply = false
            return errors.New("log mismatch at PrevIndex")
        }
    }

    if msg.Index > len(l.Entries) {
        l.Entries = append(l.Entries, msg)
    }

    if msg.CommitIndex > l.CommitIndex {
        l.CommitIndex = msg.CommitIndex
    }

    *reply = true
    return nil
}
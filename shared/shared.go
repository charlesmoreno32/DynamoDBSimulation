package shared

import (
	//    "fmt"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"crypto/md5"
	"sort"
    "encoding/binary"
	"slices"
)

const (
    MAX_NODES       =  8 //update as needed
    Z_TIME_MIN      = 10
    ROLE_FOLLOWER   =  0
    ROLE_CANDIDATE  =  1
    ROLE_LEADER     =  2
    N_REPLICAS      =  3
    VNODES_PER_NODE =  3
	FLAG_PUT        =  0
	FLAG_FETCH      =  1
	FLAG_RESPONSE   =  2
	RING_SIZE       = 4294967295 //Max Uint32 cast to int
)

/***********************************************
 ******************** STORE ********************
 ***********************************************/
type Store struct {
    Data    map[int]string // key → value
}

func NewStore() *Store {
    return &Store{
        Data:    make(map[int]string),
	}
}

func (st *Store) Put(key int, data string, reply *bool) error {
    st.Data[key] = data
    return nil
}

func (st *Store) Get(key int, reply *string) error {
    data, exists := st.Data[key]
	if exists {
		*reply = data
		return nil
	} else {
		return errors.New("Key " + fmt.Sprintf("%d", key) + " not found in database")
	}
}

func (st *Store) FetchKeysInRange(beginning int, end int, remove bool) map[int]string {
	removedKeys := make(map[int]string)
	for key, data := range st.Data {
		if key > beginning && key <= end {
			removedKeys[key] = data //add key to list
			if remove {
				delete(st.Data, key)  //remove from store
			}
		}
	}
	return removedKeys
}

/************************************************
 ********************* NODE *********************
 ************************************************/
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
    Store     Store
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



/************************************************
 ******************* PROPOSAL *******************
 ************************************************/
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

/************************************************
 ****************** MEMBERSHIP ******************
 ************************************************/
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
    m.mu.Lock()
    m.Members[payload.ID] = payload
    m.mu.Unlock()
    *reply = payload
    return nil //errors.New("\"Update\" unimplemented")
}

func (m *Membership) Get(payload int, reply *Node) error {
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

/*************************************************
 ******************** REQUEST ********************
 *************************************************/
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
    return &Requests{
        Pending: make(map[int]Request),
    }
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
    req.mu.Lock()
    req.Pending[payload.ID] = payload
    *reply = true // Does this have a point?
    req.mu.Unlock()
    return nil
}

// Listens to communication from neighboring node & returns table
func (req *Requests) Listen(ID int, reply *Membership) error {
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

/*************************************************
 ********************** LOG **********************
 *************************************************/
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

/***********************************************
 ******************** VNODE ********************
 ***********************************************/
type VNode struct {
    NodeID 	 int
	Location int
}
/*************************************************
 ****************** DYNAMO RING ******************
 *************************************************/
type DynamoRing struct {
    VNodes   []VNode
	Rng      *rand.Rand
    mu       sync.Mutex
}

func NewDynamoRing() *DynamoRing {
    return &DynamoRing{
		VNodes: []VNode{},
		Rng:    rand.New(rand.NewSource(99)), //Keep constant for now
		}
}

func (r *DynamoRing) containsLoc(location int) bool {
	for _, vnode := range r.VNodes {
		if vnode.Location == location {
			return true
		}
	}
	return false
}


func (r *DynamoRing) GetVNodes(fetch bool, reply *[]VNode) error {
	r.mu.Lock()
	*reply = r.VNodes
	r.mu.Unlock()
    return nil
}

func (r *DynamoRing) Join(nodeID int, reply *bool) error {
    r.mu.Lock() 
	//TODO
	// Choose random location on ring.
	for i := 0; i < VNODES_PER_NODE; i++ {
		location := int(r.Rng.Uint32())
		if !r.containsLoc(location) {
			vNode := VNode{nodeID, location}
			r.VNodes = append(r.VNodes, vNode)
		} else {
			i--
		}
	}

	sort.Slice(r.VNodes, func(i, j int) bool {
		return r.VNodes[i].Location < r.VNodes[j].Location
	})
	
	// Get replicas from needed nodes.
    r.mu.Unlock()
	*reply = true
	return nil
}
 
func (r *DynamoRing) GetPreferenceList(key int, reply *[N_REPLICAS]int) error {
    r.mu.Lock()
	var nodeList [N_REPLICAS]VNode
	var preferenceList [N_REPLICAS]int
	for i := range preferenceList {
        preferenceList[i] = -1
    }
	
	i := 0;
	for _, vNode := range r.VNodes {
		if vNode.Location >= key {
			contained := false
			for n := 0; n < i; n++ {
				if (nodeList[n].NodeID == vNode.NodeID) {
					contained = true //not distinct physical node
					break
				}
			}
			if !contained {
				nodeList[i] = vNode
				preferenceList[i] = vNode.NodeID
				i++
				if i == N_REPLICAS {
					break;
				}
			}
		}
	}
	if i != N_REPLICAS {
		for _, vNode := range r.VNodes {
			if vNode.Location < key {
				contained := false
				for n := 0; n < i; n++ {
					if (nodeList[n].NodeID == vNode.NodeID) {
						contained = true //not distinct physical node
						break
					}
				}
				if !contained {
					nodeList[i] = vNode
					preferenceList[i] = vNode.NodeID
					i++
					if i == N_REPLICAS {
						break;
					}
				}
			}
		}
	}
	*reply = preferenceList
    r.mu.Unlock()
	return nil
}

func (r *DynamoRing) GetReplicaList(vnode VNode, reply *[N_REPLICAS]VNode) error {
	r.mu.Lock()
	var replicaList [N_REPLICAS]VNode
	
	idx := slices.Index(r.VNodes, vnode)
	offset := 0 //variable for moving through list
	
	for i := 0; i < N_REPLICAS; i++ {
		node := r.VNodes[(idx + i + offset) % len(r.VNodes)]
	
		if node == vnode {
			break; //have gone over whole list
		}
		contained := false
		for n := 0; n < i; n++ {
			if (replicaList[n].NodeID == node.NodeID) {
				contained = true //not distinct physical node
				i-- //still haven't found a node to add
				offset++ //Move to next node in list
				break
			}
		}
		if !contained {
			replicaList[i] = node //Add node to list
		}
	}
	*reply = replicaList
	r.mu.Unlock()
	return nil
}

func (r *DynamoRing) GetPreviousList(vnode VNode, reply *[N_REPLICAS]VNode) error {
   r.mu.Lock()
	var previousList [N_REPLICAS]VNode
	
	idx := slices.Index(r.VNodes, vnode)
	offset := 0 //variable for moving through list
	
	for i := 0; i < N_REPLICAS; i++ {
		index := idx - i - offset
		if index < 0 {
			index = len(r.VNodes) + index
		}
		node := r.VNodes[index]
	
		if node == vnode {
			break; //have gone over whole list
		}
		contained := false
		for n := 0; n < i; n++ {
			if (previousList[n].NodeID == node.NodeID) {
				contained = true //not distinct physical node
				i-- //still haven't found a node to add
				offset-- //Move to next node in list
				break
			}
		}
		if !contained {
			previousList[i] = node //Add node to list
		}
	}
	*reply = previousList
	r.mu.Unlock()
	return nil
}


/************************************************
 *************** DATABASE MESSAGE ***************
 ************************************************/
type DBMessage struct {
	ToID     int //Destination Node ID
    FromID int //From Node ID
    Flag   int //PUT, FETCH, or RESPONSE
    Key    int
    Data   string
    TaskID int //FETCH & RESPONSE have same ID
}

// Allow for multiple messages
type DBMessages struct {
    Pending map[int][]DBMessage
    mu    sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewDBMessages() *DBMessages {
    return &DBMessages{
        Pending: make(map[int][]DBMessage),
    }
}

// Adds a new database message to leader's inbox (or if leader, to followers' boxes)
func (db *DBMessages) Add(payload DBMessage, reply *bool) error {
    db.mu.Lock()
    db.Pending[payload.ToID] = append(db.Pending[payload.ToID], payload) //Add to list
    *reply = true 
    db.mu.Unlock()
    return nil
}

// Listens to communication from leader (or if leader, from followers)
func (db *DBMessages) Listen(selfID int, reply *DBMessage) error {
  	db.mu.Lock()
	if len(db.Pending[selfID]) == 0 {
		db.mu.Unlock()
		return errors.New("No messages to process")
	}
	message := db.Pending[selfID][0] //Fetch top message
	db.Pending[selfID] = db.Pending[selfID][1:] //Remove top message
	db.mu.Unlock()
	*reply = message
	return nil
}

/************************************************
 *************** DATABASE MESSAGE ***************
 ************************************************/
type KeySetMsg struct {
	ToID   int
    FromID int //From Node ID
	Start  int //VNodes call function so stores/location can be returned
	End    int
	Flag   int //FETCH or RESPONSE
	KeySet map[int]string //Key Sets
}

// Allow for multiple messages
type KeySetMsgs struct {
    Pending map[int][]KeySetMsg
    mu    sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewKeySetMsgs() *KeySetMsgs {
    return &KeySetMsgs{
        Pending: make(map[int][]KeySetMsg),
    }
}

// Adds a new ks message 
func (ks *KeySetMsgs) Add(payload KeySetMsg, reply *bool) error {
    ks.mu.Lock()
    ks.Pending[payload.ToID] = append(ks.Pending[payload.ToID], payload) //Add to list
    *reply = true 
    ks.mu.Unlock()
    return nil
}

// Listens to communication from 
func (ks *KeySetMsgs) getFirstMsg(selfID int, reply *KeySetMsg) error {
  	ks.mu.Lock()
	if len(ks.Pending[selfID]) == 0 {
		ks.mu.Unlock()
		return errors.New("No messages to process")
	}
	message := ks.Pending[selfID][0] //Fetch top message
	ks.Pending[selfID] = ks.Pending[selfID][1:] //Remove top message
	ks.mu.Unlock()
	*reply = message
	return nil
}


/************************************************
 ******************* FUNCTIONS *******************
 ************************************************/
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

func HashData(data string) int {
    hash := md5.Sum([]byte(data))
	key := int(binary.BigEndian.Uint32(hash[12:16]))
	return key
}

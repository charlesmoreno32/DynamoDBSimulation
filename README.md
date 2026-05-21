# Election/Coordination Protocol (RAFT)

## Miriam Brunet, Charles Moreno, Toby Mui

Lab 3

Can Be done in groups of 1-4 people

Add consensus or leader algorithm protocol, you can add Paxos or Raft,  to your Membership lab 2

 

Coordination on a Failure Tolerant System

Implement Paxos as described in the article : Paxos Made Simple 
  2. Implement a Simplified version of Paxos Consensus protocol for leader election. RAFT.

If you implement both then you get 20 extra points towards the final

 

Create 8 nodes for this simulations

For Raft Implementation:

Every node can be : Follower, Candidate or Leader

All nodes start as followers.
If followers don’t hear from the leader in an X amount of time, then they can become candidates. Every node has a timeout Y (a random number between 150-300ms) which is the amount of time each follower has to wait until becoming candidate, if the node receives a message from the leader before this timeout expires than the timer will be reset
The candidate requests votes from other nodes (it does also vote for himself), in this case “other” nodes are going to be all nodes in the system. The candidate also waits for Z time to receive the votes and counts them at the end of this timer
If the receiving nodes hasn’t yet voted in this term, then the Node votes for the candidate


5. If the candidate gets a majority than the candidate becomes the leader

6. If two nodes become candidates at same time:



To break the tie, wait for a random amount of time and hold elections again

You can use Go routines or RPC  for the Implementation.

If you implement both algorithms, try to do some performance evaluation; who reaches consensus sooner?

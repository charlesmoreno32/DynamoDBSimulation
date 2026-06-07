package main

import (
	"DynamoDBSimulation/shared"
	"fmt"
	"io"
	"net/http"
	"net/rpc"
)

func main() {
        // create a Membership list
        leader := shared.Node{}
        nodes := shared.NewMembership()
        requests := shared.NewRequests()
		proposals := shared.NewProposal()
        raftLog := shared.NewLog()
        ring   := shared.NewDynamoRing()
        db_messages := shared.NewDBMessages()
        store := shared.NewStore()

        // register nodes with `rpc.DefaultServer`
        rpc.Register(&leader)
        rpc.Register(nodes)
        rpc.Register(requests)
		rpc.Register(proposals)
        rpc.Register(raftLog)
        rpc.Register(ring)
        rpc.Register(db_messages)
        rpc.Register(store)

        // register an HTTP handler for RPC communication
        rpc.HandleHTTP()

        // sample test endpoint
        http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
                io.WriteString(res, "RPC SERVER LIVE!")
        })

        // listen and serve default HTTP server
        fmt.Println("RPC server listening on localhost:9005")
        if err := http.ListenAndServe("localhost:9005", nil); err != nil {
                fmt.Println("server failed:", err)
        }
}

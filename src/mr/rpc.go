package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type DoTaskArgs struct {
	JobName string
	File string
	Phase jobPhase
	TaskNumber int

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int

}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ShutdownReply struct {
	Ntasks int
}

type RegisterArgs struct {
	Worker string
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", srv)
	log.Println(srv, rpcname, args, reply)
	//c, err := rpc.DialHTTP("unix", srv)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
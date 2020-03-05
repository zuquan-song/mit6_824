package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"

type Master struct {
	sync.Mutex
	address string
	registerChan chan string
	doneChan chan bool
	workers []string

	jobName string
	files []string
	nReduce int
	nMap int
	shutdown chan struct{}
	l net.Listener
	stats []int
}

// Your code here -- RPC handlers for the worker to call.

func newMaster(master string, nReduce int) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.registerChan = make(chan string)
	mr.doneChan = make(chan bool)
	mr.nReduce = nReduce
	mr.nMap = nReduce

	return
}

func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChan <- args.Worker
	}()
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (mr *Master) Wait() bool {
	return <-mr.doneChan
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Println(files)
	jobName := "wc"
	mr := newMaster("master", nReduce)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce, mr.schedule, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})
	return mr
}

func (mr *Master) run(jobName string, files []string, nReduce int, schedule func(phase jobPhase), finish func()) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce

	schedule(mapPhase)
	schedule(reductPhase)
	finish()
	//mr.merge()
	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)
	mr.doneChan <- true
}

func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}

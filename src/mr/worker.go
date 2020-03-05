package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type Worker struct {
	sync.Mutex

	name string
	Map func(string, string) []KeyValue
	Reduce func(string, []string) string
	nRPC int
	nTasks int
	l net.Listener
}

func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n", wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reductPhase:
		doReduce(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)
	}

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	wk.nTasks --
	return nil
}

func (wk *Worker) register(master string) {
	log.Println("register: ", master)
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	log.Println("register result: ", ok)
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

func RunWorker(MasterAddress string, me string, MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string, nRPC int) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpc.Register(wk)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", me)

	//l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}

	wk.l = l
	wk.register(MasterAddress)

	defer func() {
		wk.l.Close()
	}()

	for {
		err := http.Serve(l, nil)
		if err != nil {
			debug("RegistrationServer: accept error", err)
			break
		}

		//wk.Lock()
		//if wk.nRPC == 0 {
		//	wk.Unlock()
		//	break
		//}
		//wk.Unlock()
		//conn, err := wk.l.Accept()
		//if err == nil {
		//	wk.Lock()
		//	wk.nRPC --
		//	wk.Unlock()
		//	go rpc.ServeConn(conn)
		//	wk.Lock()
		//	wk.nTasks ++
		//	wk.Unlock()
		//} else {
		//	break
		//}
 	}
	debug("RunWorker %s exit\n", me)
}





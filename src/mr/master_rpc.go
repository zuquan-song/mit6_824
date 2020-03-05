package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (mr *Master) Shutdown(_, _ *struct{}) error {
	debug("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.l.Close()
	return nil
}

func (t *Master) Hi(args string, reply *string) error {
	*reply = "echo:" + args
	return nil
}

func (mr *Master) startRPCServer() {
	print("startRPCServer, register: ", mr)
	mr.address = ":1234"
	rpc.Register(mr)
	rpc.HandleHTTP()
	//os.Remove(mr.address)
	//l, e := net.Listen("unix", mr.address)
	l, e := net.Listen("tcp", mr.address)

	if e != nil {
		log.Fatal("RegistrationServer:", mr.address, " error: ", e)
	}
	mr.l = l
	go func() {
		loop:
			for {
				select {
				case <- mr.shutdown:
					break loop
				default:
				}

				err := http.Serve(l, nil)
				if err != nil {
					debug("RegistrationServer: accept error", err)
					break
				}
				//
				//conn, err := mr.l.Accept()
				//
				//if err == nil {
				//	go func() {
				//		log.Println("Serve Conn: ", conn)
				//		rpcs.ServeConn(conn)
				//		conn.Close()
				//	}()
				//} else {
				//	debug("RegistrationServer: accept error", err)
				//	break
				//}
			}
			debug("RegistrationServer: done\n")
	}()
}

func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
	debug("cleanupRegistration: done\n")
}
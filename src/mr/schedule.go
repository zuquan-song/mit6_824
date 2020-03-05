package mr

import (
	"fmt"
	"sync"
)

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reductPhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// create n tasks
	taskArgList := make([]DoTaskArgs, ntasks)
	for i := 0; i < ntasks; i ++ {
		taskArgList[i].JobName = mr.jobName
		taskArgList[i].Phase = phase
		taskArgList[i].TaskNumber = i
		taskArgList[i].NumOtherPhase = nios
		if phase == mapPhase {
			taskArgList[i].File = mr.files[i]
		}
	}

	fmt.Printf("Schedule: taskArgsList prepared.\n")

	var wg sync.WaitGroup

	for taskIndex := 0; taskIndex < ntasks; taskIndex ++ {
		wg.Add(1)
		var idleWorker string
		go func(index int) {
			defer wg.Done()
			for {
				idleWorker = <-mr.registerChan
				ok := call(idleWorker, "Worker.DoTask", &taskArgList[index], new(struct{}))
				if ok == false {
					fmt.Printf("Master: RPC %s DoTask error\n", idleWorker)
					continue
				} else {
					go func() {
						mr.registerChan <- idleWorker
					}()
					break
				}
			}
		}(taskIndex)
	}
	wg.Wait()
	fmt.Printf("Schedule %v phase done\n", phase)
}
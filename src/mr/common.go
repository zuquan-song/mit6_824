package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

type jobPhase string

const debugEnabled = false

func debug(format string, a ... interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

const (
	mapPhase jobPhase = "Map"
	reductPhase jobPhase = "Reduce"
)

func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapFunc func(file string, contents string) []KeyValue)  {

	dat, errInput := ioutil.ReadFile(inFile)

	if errInput != nil {
		log.Fatal(errInput)
	}

	outFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	defer func() {
		for _, file := range outFiles {
			file.Close()
		}
	}()

	for i := range outFiles {
		var errOutput error
		filename := reduceName(jobName, mapTaskNumber, i)
		outFiles[i], errOutput = os.Create(filename)

		if errOutput != nil {
			log.Fatal(errOutput)
		}
		encoders[i] = json.NewEncoder(outFiles[i])
	}

	mapRes := mapFunc(inFile, string(dat))

	for _, kv := range mapRes {
		index := ihash(kv.Key) % nReduce
		encoders[index].Encode(&kv)
	}
}

func doReduce(jobName string, reduceTaskNumber int, nMap int, reduceFunc func(key string, values []string) string) {
	keyValues := make(map[string][] string)

	i := 0
	for i < nMap {
		fileName := reduceName(jobName, i, reduceTaskNumber)

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := enc.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		i ++
	}

	var keys []string
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(mergeName(jobName, reduceTaskNumber))
	defer func() {
		file.Close()
	}()

	if err != nil {
		fmt.Printf("reduce merge file:%s can't open\n", mergeName(jobName, reduceTaskNumber))
		return
	}

	enc := json.NewEncoder(file)
	for _, k := range keys {
		enc.Encode(KeyValue{k, reduceFunc(k, keyValues[k])})
	}
}
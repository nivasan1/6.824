package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"sync"

	"github.com/Workiva/go-datastructures/threadsafe/err"
	"golang.org/x/sync/errgroup"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KvWrap struct {
	kvs []KeyValue
}

// KvWrap implements the
func (kv *KvWrap) Len() int {
	return len(kv.kvs)
}

func (kv *KvWrap) Less(i, j int) bool {
	// compare keys using string Comparator
	compare := strings.Compare((kv.kvs)[i].Key, kv.kvs[j].Key)
	// kvs.key[i] < kvs.Key[j]
	if compare <= 0 {
		return true
	}
	// kvs.Key[i] >= kvs.Key[j]
	return false
}

func (kv *KvWrap) Swap(i, j int) {
	kv.kvs[i], kv.kvs[j] = kv.kvs[j], kv.kvs[i]
}

//file wrapper is a mutex around a file
//this is used so that we can pass
type EncoderWrap struct {
	// mutex handler into *file
	mut sync.Mutex
	// underlying encoder
	enc *json.Encoder
	// underlying file obj
	file *os.File
}

// convenience method for locking handle to file
func (fw *EncoderWrap) Lock() {
	fw.mut.Lock()
}

// convenience method for releasing handle to file
func (fw *EncoderWrap) UnLock() {
	fw.mut.Unlock()
}

// convenience method for instantiating multiple EncoderWraps
func NewEncoderWrap(taskName string, taskNum uint) (*EncoderWrap, error) {
	file, err := CreateFile(taskName, taskNum)
	if err != nil {
		return nil, err
	}
	return &EncoderWrap{
		mut:  sync.Mutex{},
		enc:  json.NewEncoder(file),
		file: file,
	}, nil
}

//closes the EncoderWrap
func (ec *EncoderWrap) Close() error {
	err := ec.file.Close()
	return err
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

// convenience method around KeyValue to determine bucket placement
func (kv KeyValue) WhichBucket(numReduce uint) uint {
	return uint(ihash(kv.Key)) % numReduce
}

//
// main/mrworker.go calls this function. When the worker is spawned immediately make a request to the Coordinator
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// launch worker by requesting job from the co-ordinator
	req := &Request{}
	res := &Response{}
	err := call("Coordinator.RequestJob", req, res)
	if err != nil {
		log.Fatalf("mr::Worker: error requesting job")
	}
	
	// now in an endless loop receive calls from the co-ordinator and process them
	for {
		// handle request from the Co-ordinator
		switch res.jobStatus {
		case NOT_STARTED:
			// handle map, return the updated Request to the Co-Ordinator
			_, err = res.HandleMap(mapf)			
			if err != nil { 
				log.Fatalf("worker.go::Worker: %v", err)
			}
		case MAP_DONE:

		}
	}

}

// Handle map method for processing map requests, takes as argument a request sent from the co-ordinator, handles the request, 
//returns any error emitted during processing of request, and returns the response to the co-ordinator
// Request Returned should be sent to the co-ordinator for subsequent processing
func (res *Response) HandleMap(mapf func (string, string)[]KeyValue) (*Request, error) { 
	// fail if no filename has been provided
	if res.fname == "" {
		return  &Request{}, errors.New(fmt.Sprint("worker.go::Worker: no filename returned", res.fname))
	}

	// fname exists, let's open it
	contents, err := ReadContents(res.fname)
	if err != nil {
		return &Request{}, errors.New(fmt.Sprintf("worker.go::Worker: error opening/reading contents of file: %v", err.Error()))
	}
	// instantiate wait group object for grouping go-routines
	var eg *errgroup.Group

	// create encoder wraps in paralell
	encWraps := make([]*EncoderWrap, res.taskNum)
	for i := 0; i < res.taskNum; i++ {
		//use err group so we can wait on all tasks to finish and then
		eg.Go(func() error {
			encWraps[i], err = NewEncoderWrap(res.fname, uint(i))
			return err
		})
	}
	// kvs un-bucketed
	kvs := mapf(res.fname, contents)
	bucketSize := len(kvs) / res.taskNum // kvs per bucket
	// block for all Enc Wrappers to be created fail if any file creators fail
	err = eg.Wait()
	if err != nil {
		return &Request{},  errors.New("worker.go::fatal error creating files")
	}

	// first sort unstructured data, that way, we can avoid doing this operation many times
	sort.Sort(&KvWrap{kvs})

	ecList := encWrapsList{
		kvs:     kvs,
		list:    encWraps,
		taskNum: uint(res.taskNum),
	}
	// create sync waitgroup to group tasks into seperate go-routines
	// allocate tasks per go-routine via bucket size
	for i := 0; i < len(encWraps); i++ {
		eg.Go(
			func() error {
				err = ecList.WriteBuckets(i*bucketSize, (i+1)*bucketSize)
				return err
			})
	}
	eg.Wait()
	// now close all files
	for i := 0; i < len(encWraps); i++ {
		eg.Go(func() error {
			return encWraps[i].Close()
		})
	}
	if err = eg.Wait(); err != nil {
		return &Request{}, errors.New(fmt.Sprint("worker.go::error closing encoder wraps: %v", err.Error()))
	}
	return &Request{}, nil
}


type encWrapsList struct {
	kvs     []KeyValue
	list    []*EncoderWrap
	taskNum uint
}

func (ec encWrapsList) WriteBuckets(idxStart, idxEnd int) error {
	var err error
	for i := idxStart; i < idxEnd; i++ {
		// list is sorted, so batch all requests with same key into same write
		// writes are done synchronously, so grab the handle
		hashKey := ec.kvs[i].WhichBucket(ec.taskNum)
		ec.list[hashKey].Lock()
		// group tasks of writing KVs with same key to the same encoded file
		for ; strings.Compare(ec.kvs[i].Key, ec.kvs[i+1].Key) == 0; i++ {
			if err = ec.list[hashKey].enc.Encode(&ec.kvs[i]); err != nil {
				return err
			}
		}
		// when we are done writing to file release handle
		ec.list[hashKey].UnLock()
	}
	return nil
}

//
//read the contents of a file from it's fname, fails if the file doesn't exist
// ensure to release the resources after we use the file
// bubbles up errors into parent function for propoer error handling
//
func ReadContents(fname string) (string, error) {
	// open and read file in O_RDONLY mode
	contents, err := os.ReadFile(fname)
	if err != nil {
		return "", err
	}

	// convert decode byteArray into string and return the value
	return string(contents), nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// dial socket and receive the RPC interface for co-ordinator
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, &args, &reply)
	if err == nil {
		return nil
	}

	return err
}

// create file for writing, meant to be opened in synced goroutine
// bubbles any errors encountered in the process of file creation
//file closure is delegated to caller
func CreateFile(taskName string, taskNum uint) (*os.File, error) {
	// format the temp file name according to task and task number
	s := fmt.Sprintf("temp-%s-%d", taskName, taskNum)
	//open the file to write to
	file, err := os.Create(s)
	if err != nil {
		return nil, err
	}

	return file, nil
}

//write json object to file

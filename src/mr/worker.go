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
	File *os.File
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
func NewEncoderWrap(taskName string, taskNum int) (*EncoderWrap, error) {
	file, err := CreateFile(taskName, taskNum)
	if err != nil {
		return nil, err
	}
	return &EncoderWrap{
		mut:  sync.Mutex{},
		enc:  json.NewEncoder(file),
		File: file,
	}, nil
}

//closes the EncoderWrap
func (ec *EncoderWrap) Close() error {
	err := ec.File.Close()
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
func (kv KeyValue) WhichBucket(numReduce int) int {
	return ihash(kv.Key) % numReduce
}

/*
	Function takes in a slice of KVs, an array of EncWrappers,
	Handles encoding of all KVs into requisite files,
*/
func WriteBuckets(kvs []KeyValue, files []*EncoderWrap) error {
	var err error
	for i := 0; i < len(kvs); i++ {
		// list is sorted, so batch all requests with same key into same write
		// writes are done synchronously, so grab the handle
		hashKey := kvs[i].WhichBucket(len(files))
		files[hashKey].Lock()
		// group tasks of writing KVs with same key to the same encoded file
		for {
			if err = files[hashKey].enc.Encode(&kvs[i]); err != nil {
				files[hashKey].UnLock()
				return err
			}
			// only increment if we are not at the end of the list
			if i >= len(kvs)-1 {
				files[hashKey].UnLock()
				return nil
			}
			if !(strings.Compare(kvs[i].Key, kvs[i+1].Key) == 0) {
				break
			}
			i++
		}
		files[hashKey].UnLock()
	}
	return nil
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
		switch res.JobStatus {
		case NOT_STARTED:
			// handle map, return the updated Request to the Co-Ordinator
			req, err = res.HandleMap(mapf)
			if err != nil {
				log.Fatalf("worker.go::Worker: %v", err)
			}
		case MAP_DONE:
			
		}
		// request new task
	}

}

// Handle map method for processing map requests, takes as argument a request sent from the co-ordinator, handles the request,
//returns any error emitted during processing of request, and returns the response to the co-ordinator
// Request Returned should be sent to the co-ordinator for subsequent processing
func (res *Response) HandleMap(mapf func(string, string) []KeyValue) (*Request, error) {
	// fail if no filename has been provided
	if res.Fname == "" {
		return &Request{
			CurJobFname:  res.Fname,
			CurJobStatus: ERROR,
		}, errors.New(fmt.Sprint("worker.go::Worker: no filename returned", res.Fname))
	}

	// fname exists, let's open it
	contents, err := ReadContents(res.Fname)
	if err != nil {
		return &Request{
			CurJobFname:  res.Fname,
			CurJobStatus: ERROR,
		}, errors.New(fmt.Sprintf("worker.go::Worker: error opening/reading contents of file: %v", err.Error()))
	}
	// instantiate wait group object for grouping go-routines
	eg := &errgroup.Group{}

	// create encoder wraps in paralell
	encWraps := make([]*EncoderWrap, res.TaskNum)
	for i := 0; i < res.TaskNum; i++ {
		encWraps[i], err = NewEncoderWrap(res.Fname, i)
		if err != nil {
			return &Request{
				CurJobFname:  res.Fname,
				CurJobStatus: ERROR,
			}, err
		}
	}
	// this must be finished in the event of any unexpected events
	defer func() {
		// now close all files
		for i := 0; i < len(encWraps); i++ {
			encWraps[i].Close()
		}
	}()
	// kvs un-bucketed
	kvs := mapf(res.Fname, contents)
	bucketSize := len(kvs) / res.TaskNum // kvs per bucket
	// block for all Enc Wrappers to be created fail if any file creators fail
	err = eg.Wait()
	if err != nil {
		return &Request{
			CurJobFname:  res.Fname,
			CurJobStatus: ERROR,
		}, errors.New("worker.go::fatal error creating files")
	}

	// first sort unstructured data, that way, we can avoid doing this operation many times
	sort.Sort(&KvWrap{kvs})

	var wg sync.WaitGroup
	// create sync waitgroup to group tasks into seperate go-routines
	// allocate tasks per go-routine via bucket size
	wg.Add(len(encWraps))
	errChan := make(chan error)
	// ensure that errChan is closed
	defer close(errChan)
	// iteration variable is re-used, ensure that the value is passed to go-routine,
	// not the reference
	for i := 0; i < len(encWraps); i++ {
		go func(i int) {
			// handle reading from the keys concurrently
			err := WriteBuckets(kvs[bucketSize*i:bucketSize*(i+1)], encWraps)
			if err != nil {
				// send error through channel
				errChan <- err
				wg.Done()
			}
			errChan <- nil
			wg.Done()
		}(i)
	}
	// order receipt from corresponding go-routines, and check if any errors exist
	for i := 0; i < len(encWraps); i++ {
		if <-errChan != nil {
			return &Request{
				CurJobFname:  res.Fname,
				CurJobStatus: ERROR,
			}, errors.New(fmt.Sprintf("worker.go::error closing encoder wraps: %v", err.Error()))
		}
	}
	// block until all processes have finished
	wg.Wait()

	// return results to the co-ordinator indicating that the map task has been completed
	return &Request{
		CurJobFname:  res.Fname,
		CurJobStatus: MAP_DONE,
	}, nil
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
		log.Fatalf("dialing: %s", err.Error())
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
func CreateFile(taskName string, taskNum int) (*os.File, error) {
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

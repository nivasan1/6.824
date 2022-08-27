package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// enum reporesenting the status associated with each job
const (
	NOT_STARTED = iota // indication that the job has yet to start
	MAP_DONE    = iota
	REDUCE_DONE = iota
)

type Coordinator struct {
	//set of processes detailed w/ bool to determine status of job
	procs map[string]int
	// set of intermediate k/v pairs
	kvs []KeyValue
	// number of "buckets" for which mapped data will be written to
	tasks int
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	// iterate through jobs and determine which are left to be done
	for _, status := range c.procs {
		if status != REDUCE_DONE {
			ret = false
		}
	}

	return ret
}

/*
	Request Job is called from the worker upon starting a job
	First updates status of job for which the requesting worker has worked on
	Then, if the worker has recently completed a Map job, read the written file
*/
func (c *Coordinator) RequestJob(req *Request, res *Response) *Response {
	// update status of current job of worker
	c.procs[req.curJobFname] = req.curJobStatus
	for job, status := range c.procs {
		if status != REDUCE_DONE {
			// increment status of job for jobs that have not been yet finished
			status++
			// return the response to indicate to worker what the work to be finished is
			return &Response{
				fname:     job,
				jobStatus: status,
				taskNum:   c.tasks,
			}
		}
	}

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// instantiate empty map of filenames to status
	procs_tmp := map[string]int{}
	kvs := []KeyValue{}
	for _, file := range files {
		procs_tmp[file] = 0 // set job to un-completed for each file in the files range
	}

	// instantiate the coordinator
	c := Coordinator{
		procs: procs_tmp,
		kvs:   kvs,
		tasks: nReduce,
	}

	// launch co-ordinator server to receive requests from the workers
	c.server()
	return &c
}

package worker_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"encoding/json"

	"6.824/mr"
	"6.824/mrapps"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

type WorkerTestSuite struct {
	suite.Suite

	//map function
	mapf func(string, string) []mr.KeyValue
	// file to read data from
}

// test that the creation of files is working as expected
func TestHash(t *testing.T) {
	type testArgs struct {
		key1     mr.KeyValue
		key2     mr.KeyValue
		numTasks uint
	}

	testCases := []struct {
		name string
		args testArgs
		pass bool
	}{
		{
			"test that 2 equal kvs hash to same value",
			testArgs{
				mr.KeyValue{Key: "hi", Value: ""},
				mr.KeyValue{Key: "hi", Value: ""},
				12,
			},
			true,
		},
		{
			"test that 2 non-equal kvs do not hash to same value",
			testArgs{
				mr.KeyValue{Key: "hi", Value: ""},
				mr.KeyValue{Key: "bye", Value: ""},
				12,
			},
			false,
		},
	}
	for _, tc := range testCases {
		fmt.Printf("Running: %s\n", tc.name)
		hash1 := tc.args.key1.WhichBucket(int(tc.args.numTasks))
		hash2 := tc.args.key2.WhichBucket(int(tc.args.numTasks))
		require.Equal(t, hash1 == hash2, tc.pass, "test failed")
	}
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(WorkerTestSuite))
}

func (suite *WorkerTestSuite) SetupTest() {
	// set map function for testing
	suite.mapf = mrapps.Map
}

// test methods for reading and creating files, create file based upon taskName and bucket number, should be able to open, read and close file
func (suite *WorkerTestSuite) TestFileCreateRead() {
	type testArgs struct {
		taskName string // file name
		taskNum  int    // task number
		contents string
		setup    func(fname, contents string, taskNum int) (string, error)
	}

	testCases := []struct {
		name       string
		args       testArgs
		expectPass bool
	}{
		{
			"temp-taskName-taskNum should be created with no error",
			testArgs{
				"taskName",
				1,
				"contents",
				func(taskName, contents string, taskNum int) (string, error) {
					// first create the file to read
					file, err := mr.CreateFile(taskName, taskNum)
					if err != nil {
						return "", err
					}
					// file exists, let's make sure to close it
					defer file.Close()
					// write contents to the file
					_, err = file.WriteString(contents)
					if err != nil {
						return "", err
					}
					return mr.ReadContents(file.Name())
				},
			},
			true,
		},
	}

	for _, tc := range testCases {
		fmt.Printf("Running: %s\n", tc.name)
		initTime := time.Now()
		// retrieve file name
		fname := fmt.Sprintf("temp-%s-%d", tc.args.taskName, tc.args.taskNum)
		// retrieve contents written to file in test
		out, err := tc.args.setup(tc.args.taskName, tc.args.contents, tc.args.taskNum)

		if tc.expectPass {
			// expect no error to occur in file creation / reading
			suite.Require().NoError(err)
			// expect contents to be what was read from file
			suite.Require().Equal(out, tc.args.contents)
			// delete the file read
			err := os.Remove(fname)
			if err != nil {
				panic(err)
			}
		} else {
			suite.Require().Error(err)
		}
		fmt.Printf("testCase: %s: %v\n", tc.name, time.Since(initTime))
	}
}

/*
	Write Buckets test
*/
/*
	Test that under arbitrary map function,
		1. If a given file does not exist, emit failure (file is written to temp-taskName-taskNum)
		2. Open file ./txt/x.txt, call map, retrieve key-values, and write to correct files
		3. Test with single thread first, then move to multiple
*/
func (suite *WorkerTestSuite) TestWriteBuckets() {
	type testArgs struct {
		taskName   string
		taskNum    uint
		numThreads int
		// given taskName, number of tasks, and number of threads to process data, return array of written files, and an error if present
		setup func(fname, taskName string, taskNum uint, numThreads int) ([]*os.File, error, int)
	}

	testCases := []struct {
		name       string
		args       testArgs
		expectPass bool
	}{
		{
			"single thread writes should all go through - pass",
			testArgs{
				"pg-grimm.txt",
				uint(1),
				1,
				func(fname, taskName string, taskNum uint, numThreads int) ([]*os.File, error, int) {
					// instantiate EncoderWrap
					enc, err := mr.NewEncoderWrap(taskName, 1)
					if err != nil {
						return []*os.File{enc.File}, err, 0
					}
					// make sure to close EncoderWrap's file
					defer enc.Close()
					// process fname's contents and return ketValuePairs
					kvs := ProcessData(suite.mapf, fname)

					// finally Write kvs to numBuckets
					mr.WriteBuckets(kvs, []*mr.EncoderWrap{enc})

					return []*os.File{enc.File}, nil, len(kvs)
				},
			},
			true,
		},
		{
			"number of threads and task number are equal, and greater than 1 - pass",
			testArgs{
				"pg-grimm.txt",
				uint(2),
				2,
				func(fname, taskName string, taskNum uint, numThreads int) ([]*os.File, error, int) {
					// instantiate 2 EncoderWraps
					enc := make([]*mr.EncoderWrap, 2)
					files := make([]*os.File, 2)
					var err error
					for i := 1; i < int(taskNum)+1; i++ {
						enc[i-1], err = mr.NewEncoderWrap(taskName, i)
						files[i-1] = enc[i-1].File
						if err != nil {
							return files, err, 0
						}
					}
					// close both encoder wraps
					defer enc[0].Close()
					defer enc[1].Close()
					// process fname's contents and return KeyValue Pairs
					kvs := ProcessData(suite.mapf, fname)
					// create errgroup
					var eg errgroup.Group
					eg.Go(
						func() error {
							// handle reading from kvs concurrently
							err := mr.WriteBuckets(kvs[:len(kvs)/2], enc)
							return err
						})
					// spawn second thread to handle reads
					eg.Go(
						func() error {
							// handle second batch of reads concurrently
							err := mr.WriteBuckets(kvs[len(kvs)/2:], enc)
							return err
						})
					// await for err from both threads
					err = eg.Wait()
					if err != nil {
						return files, err, 0
					}

					return files, nil, len(kvs)
				},
			},
			true,
		},
	}

	for _, tc := range testCases {
		initTime := time.Now()
		//run setup test with given arguments
		files, err, kvsLen := tc.args.setup(tc.args.taskName, tc.args.taskName, tc.args.taskNum, 1)
		// require that there was no error in execution
		suite.Require().NoError(err)
		// read contents and print them
		// ensure that all files are removed and that the file names are those expected
		defer func() {
			for _, file := range files {
				// check that all values in each file has been mapped to the correct value
				name := file.Name()
				os.Remove(name)
			}
		}()
		i := 0
		// now check that the number of kvs and each bucket has been written correctly
		for idx, file := range files {
			// check that files have been written to the correct places
			suite.Require().Equal(file.Name(), fmt.Sprintf("temp-%s-%d", tc.args.taskName, idx+1))
			// now check that hashes have been made correctly
			checkBucket, numKvs := checkHashes(file, int(tc.args.taskNum), idx)
			suite.Require().True(checkBucket)
			// increment total number of kvs
			i += numKvs
		}
		suite.Require().Equal(i, kvsLen)
		fmt.Printf("Running: %s\n", tc.name)
		fmt.Printf("took: %d\n", time.Since(initTime))
	}
}

// Test Handle Map
// 1. Test failure on non-empty files
// 2. Test on Single THread
// 3. Test on multiple threads
func (suite *WorkerTestSuite) TestHandleMap() {
	type testArgs struct {
		fname string
		resp  mr.Response
		setup func(*mr.Response) (*mr.Request, error)
	}

	testCases := []struct {
		name       string
		args       testArgs
		expectReq  mr.Request
		expectPass bool
	}{
		{
			"if the response name is nil - fail",
			testArgs{
				"pg-grimm.txt",
				mr.Response{
					Fname:     "",
					JobStatus: 1,
					TaskNum:   1,
				},
				func(resp *mr.Response) (*mr.Request, error) {
					req, err := resp.HandleMap(suite.mapf)
					return req, err
				},
			},
			mr.Request{
				CurJobFname:  "",
				CurJobStatus: mr.ERROR,
			},
			false,
		},
		{
			"response with pg-grimm.txt fname, results in pg-grimm.txt request - pass",
			testArgs{
				"pg-grimm.txt",
				mr.Response{
					Fname:     "pg-grimm.txt",
					JobStatus: mr.NOT_STARTED,
					TaskNum:   1,
				},
				func(resp *mr.Response) (*mr.Request, error) {
					req, err := resp.HandleMap(suite.mapf)
					return req, err
				},
			},
			mr.Request{
				CurJobFname:  "pg-grimm.txt",
				CurJobStatus: mr.MAP_DONE,
			},
			true,
		},
		{
			"response with pg-grimm.txt fname, results in pg-grimm.txt request, multiple threads - pass",
			testArgs{
				"pg-grimm.txt",
				mr.Response{
					Fname:     "pg-grimm.txt",
					JobStatus: mr.NOT_STARTED,
					TaskNum:   2,
				},
				func(resp *mr.Response) (*mr.Request, error) {
					return resp.HandleMap(suite.mapf)
				},
			},
			mr.Request{
				CurJobFname:  "pg-grimm.txt",
				CurJobStatus: mr.MAP_DONE,
			},
			true,
		},
	}

	for _, tc := range testCases {
		// print name of test
		fmt.Printf("Running: %s\n", tc.name)
		// run setup for test
		req, errSetup := tc.args.setup(&tc.args.resp)
		// now handle test results
		suite.Require().Equal(tc.expectReq, *req)
		// ok, now let's process the output from these processes
		kvs := ProcessData(suite.mapf, tc.args.fname)
		// clean up test, and run general checks
		fail, err := suite.iterateFiles(tc.args.fname, len(kvs), tc.args.resp.TaskNum, tc.expectPass)
		if tc.expectPass {
			suite.Require().True(fail)
			suite.Require().NoError(errSetup)
			suite.Require().NoError(err)
		} else {
			suite.Require().False(fail)
			suite.Require().Error(errSetup)
			suite.Require().NoError(err)
		}
	}
}

// iterate files, iteratively searches through files marked temp-fname-id,
// fail if tasknum temp files haven't been created
// fail if any of the intermediate files are written to incorrectly
// fail if the total number of kv pairs is less than len(kv)
func (suite *WorkerTestSuite) iterateFiles(fname string, numPairs, taskNum int, isPass bool) (bool, error) {
	// read the working directory
	files, err := os.ReadDir(".")
	if err != nil {
		return false, err
	}
	// if len of files is not = taskNum, fail
	if len(files) != numPairs {
		return false, nil
	}
	// iterate through dirEntries, and open each files
	// dirEntries is sorted, so the indices map to thread task
	i := 0
	numKvs := 0
	fail := false
	for idx, file := range files {
		expectName := fmt.Sprintf("temp-%s-%d", fname, idx)

		if file.Name() == expectName {
			// ok this is a file produced from the test run
			if !isPass {
				//failing test, just remove file
				err := os.Remove(expectName)
				if err != nil {
					return false, err
				}
			}
			// this is a passing test
			file, err := os.Open(expectName)
			if err != nil {
				return false, err
			}
			// add another file
			i++
			// whew, file is closed

			// ok check hashes now
			fail, temp := checkHashes(file, taskNum, idx)
			// checkHashes fails?
			if !fail {
				fail = false
			}
			numKvs += temp
			if err = file.Close(); err != nil { 
				return false, err
			}
		}
	}

	fmt.Printf("numKvs: %d, numPairs: %d", numKvs, numPairs)

	if numKvs != numPairs {
		fail = false
	}

	if i != taskNum {
		fail = false
	}

	return fail, nil
}

// checkHashes will read through the mapped data and determine
// that all keys have been placed in the correct files and in the correct order
// check map will iterate through the files and determine that the key
// / value pairs are of correct cardinality
func checkHashes(file *os.File, numReduce, taskNum int) (bool, int) {
	// instantiate json decoder
	i := 0
	// open the file, and immediately defer it's closure
	file, err := os.Open(file.Name())
	if err != nil {
		log.Fatalf(err.Error())
	}
	// defer closure of file
	defer file.Close()
	dec := json.NewDecoder(file)

	for {
		var kv mr.KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		//increment idx of number of kvs written to file
		i++
		// this output was written to the wrong file, fail
		if kv.WhichBucket(numReduce) != taskNum {
			return false, i
		}
	}
	return true, i
}

// testing function to read a files contents and process them into an array of key value pairs
// returns the processed keyValue pairs, as well as the contents (stringified)
func ProcessData(mapf func(string, string) []mr.KeyValue, fname string) []mr.KeyValue {
	// read contents of file and return error if there is one
	contents, err := mr.ReadContents(fname)
	if err != nil {
		return []mr.KeyValue{}
	}

	// return processed data from files
	return mapf("", contents)
}

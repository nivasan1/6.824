package worker_test

import (
	"fmt"
	"os"
	"testing"

	"6.824/mr"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WorkerTestSuite struct {
	suite.Suite
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
		hash1 := tc.args.key1.WhichBucket(tc.args.numTasks)
		hash2 := tc.args.key2.WhichBucket(tc.args.numTasks)
		require.Equal(t, hash1 == hash2, tc.pass, "test failed")
	}
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(WorkerTestSuite))
}

// test methods for reading and creating files, create file based upon taskName and bucket number, should be able to open, read and close file
func (suite *WorkerTestSuite) TestFileCreateRead() {
	type testArgs struct {
		taskName string // file name
		taskNum  uint   // task number
		contents string
		setup    func(fname, contents string, taskNum uint) (string, error)
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
				func(taskName, contents string, taskNum uint) (string, error) {
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
	}
}

// Test Map method
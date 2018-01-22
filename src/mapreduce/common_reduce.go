package mapreduce

import (
	"os"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

type ByKey []KeyValue

func (k ByKey) Len() int {
	return len(k)
}
func (k ByKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
func (k ByKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	out, err := os.Create(outFile)
	if err != nil {
		fmt.Print(err)
	}
	outJsonEncoder := json.NewEncoder(out)

	var kvs []KeyValue
	for mapTaskNumber := 0; mapTaskNumber < nMap; mapTaskNumber++ {
		fileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		kvs = append(kvs, readIntermediateFile(fileName)...)
	}

	sort.Sort(ByKey(kvs))

	var valuesOfSameKey []string
	for i := 0; i < len(kvs); i++ {
		valuesOfSameKey = append(valuesOfSameKey, kvs[i].Value)
		if (i == len(kvs) - 1) || (kvs[i].Key != kvs[i + 1].Key)  {
			reduceValue := reduceF(kvs[i].Key, valuesOfSameKey)
			outJsonEncoder.Encode(&KeyValue{kvs[i].Key, reduceValue})
			valuesOfSameKey = []string{}
		}
	}
	out.Close()
}

func readIntermediateFile(fileName string) []KeyValue {
	inFile, err := os.Open(fileName)
	if err != nil {
		fmt.Print(err)
	}
	inJsonDecoder := json.NewDecoder(inFile)
	var results []KeyValue
	for {
		var kv KeyValue
		if err := inJsonDecoder.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			fmt.Print(err)
		}
		results = append(results, kv)
	}
	return results
}
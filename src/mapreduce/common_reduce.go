package mapreduce

import (
    "os"
    "sort"
    "fmt"
    "encoding/json"
)

type ByKey []KeyValue

func (a ByKey) Len() int { return len(a) }
func (a ByKey) Less(l, r int) bool { return a[l].Key < a[r].Key }
func (a ByKey) Swap(l, r int) { a[r], a[l] = a[l], a[r] }

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
    fmt.Fprintf(os.Stderr, "REDUCER %d processing %d files into %s\n", reduceTaskNumber, nMap, outFile)
    // prepare output encoder
    w, err := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0777)
    echeck(err)
    enc := json.NewEncoder(w)
    if enc == nil {
        panic("open output encoder failed")
    }
    // parse and sort key-values from input
    kv_pairs := make([]KeyValue, 0, 11)
    for m := 0; m < nMap; m++ {
        f, err := os.Open(reduceName(jobName, m, reduceTaskNumber))
        echeck(err)
        d := json.NewDecoder(f)
        if d == nil {
            panic("json.NewDecoder() nil")
        }
        var kv KeyValue
        for err = d.Decode(&kv); err == nil; err = d.Decode(&kv) {
            kv_pairs = append(kv_pairs, kv)
        }
    }
    sort.Sort(ByKey(kv_pairs))
    // merge and call reducer function for each record
    j := 0
    length := len(kv_pairs)
    for i := 1; i < length; i++ {
        if kv_pairs[i].Key != kv_pairs[j].Key {
            values := make([]string, 0, i - j)
            for k := j; k < i; k++ {
                values = append(values, kv_pairs[k].Value)
            }
            enc.Encode(KeyValue{kv_pairs[j].Key, reduceF(kv_pairs[j].Key, values)})
            j = i
        }
    }
    if j < length {
        values := make([]string, 0, length - j)
        for k := j; k < length; k++ {
            values = append(values, kv_pairs[k].Value)
        }
        enc.Encode(KeyValue{kv_pairs[j].Key, reduceF(kv_pairs[j].Key, values)})
    }
    // done
    w.Sync()
    w.Close()
}

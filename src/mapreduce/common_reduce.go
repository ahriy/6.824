package mapreduce

import (
	"fmt"
	"encoding/json"
	"os"
	"log"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	//
	fmt.Println("=================== Reduce Stage ===================")

	for m := 0; m < nMap; m++ {
		var kvs []KeyValue
		infile, err := os.Open(reduceName(jobName, m, reduceTask))
		if err != nil {
			log.Println(err)
			return
		}
		dec := json.NewDecoder(infile)

		//for err = dec.Decode(&kv); err == nil; {
		//	fmt.Printf("key is %s, value is %s\n", kv.Key, kv.Value)
		//}
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				infile.Close()
				log.Println(err)
				return
			}
			kvs = append(kvs, kv)
			//fmt.Printf("key is %v, value is %v\n", kv.Key, kv.Value)

		}
		if len(kvs) == 0 {
			infile.Close()
			log.Printf("infile %s: kvs is empty!\n", reduceName(jobName, m, reduceTask))
			return
		}
		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].Key < kvs[j].Key
		})

		// prepare to write the result to merge file
		outfile, err := os.Create(outFile)
		fmt.Printf("outFile is %s\n", outFile)
		if err != nil {
			infile.Close()
			log.Println(err)
			return
		}
		enc := json.NewEncoder(outfile)

		// sort kvs, so we can assemble kv with same key
		oldKey := kvs[0].Key
		var values []string
		for i, kv := range kvs {
			if kv.Key != oldKey {
				enc.Encode(KeyValue{oldKey, reduceF(oldKey, values)})
				oldKey = kv.Key
				values = values[:0]
			}
			values = append(values, kv.Value)
			if i == len(kvs) {
				enc.Encode(KeyValue{oldKey, reduceF(oldKey, values)})
			}
		}
		//fmt.Println(kvs)
		infile.Close()
	}

}

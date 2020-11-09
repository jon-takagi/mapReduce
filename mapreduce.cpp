#include "mapreduce.hh"
#include <map>
#include <vector>
#include <mutex>
#include <utility>
#include <thread>

using key_t = std::string;
using value_set_t = std::pair<vector<std::string>, std::mutex>;
using pdata_map_t = std::map<key_t, value_set_t>;
using pdata_partition_t = std::pair<pdata_map_t, std::mutex>;
using pdata_t = std::vector<pdata_partition_t>;

pdata_t* processed_data;
// map matches keys (as strings) to a vector of values and a mutex for the vector
// map has a mutex for adding keys to the map

// should be a vector of <mutex, map> pairs, where the maps send strings to <mutex, vector<string>> pairs
// each partition number gets a mutex and a bunch of key-value pairs
// hashpartition sends keys to numbers in the num_partitions
// each reducer gets a single key

/*
  map_worker function:
    inputs: map (the function we were passed), the todo vector, and the todo todo_mutex
      repeatedly locks the todo_mutex
        if work remains:
          grab work
          unlock mutex
          do work
        otherwise:
          unlock mutex
          end
    rval: void
*/
void map_worker(MapReduce::mapper_t map, std::vector<std::string>* todo, std::mutex* todo_mutex) {
    while (1) {
        todo_mutex->lock();
        if (todo.size() > 0) {
            std::string task = todo->pop();
            todo_mutex->unlock();
            map(task);
        } else {
            todo_mutex->unlock()
            break;
        }
    }
}

const std::string get_next(const std::string& key, int partition_number) {
    std::vector<std::string> values_of_key = *processed_data[partition_number].first[key].first;
    if (values_of_key.size() > 0) {
        return values_of_key.pop();
    } else {
        return "";
    }
}

void reduce_worker(MapReduce::reducer_t reduce, int partition) {
    // loop through the map at that partition
    // call reduce on each of the keys in order
    std::map kv_map = &processed_data[partition].first;
    for(std::map<key_t,value_set_t>::iterator iter = kv_map.begin(); iter != myMap.end(); ++iter)
    {
        key_t k =  iter->first;
        reduce(k, get_next, partition);
        // ignore value
        // Value v = iter->second;
    }
}

namespace MapReduce {
    void MR_Emit(const std::string& key, const std::string& value) { // called by Map - this adds a key, value pair to some centralized
        // todo: all of this
        // put the key and value into processed_data
        int partition = ; // todo: how do we figure this out?
        // get the map in the partition
        // see if it has the key
        // if it does,
    }
    unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions) {
        unsigned long hash = 5381;
        int c;
        while ((c = *(key++)) != '\0')
            hash = hash * 33 + c;
        return hash % num_partitions;
    }

    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition) {
      processed_data = new pdata_t();
      // create num_mappers mapping threads
      // each mapper thread grabs a task from the vector, calls Map on it
      // then create num_reducers reducing threads
      // each reducer thread calls Reduce on the keys in its partition, in order
      // get_next needs to be an iterator
      for (int i = 0; i < num_reducers; i++) { // hopefully this works to set things up
          processed_data->push_back(pdata_partition_t(pdata_map_t(), std::mutex());
      }
      // set up task vector;
      std::vector<std::string> todo;
      std::mutex todo_mutex;
      for (int i = 1; i < argc; i++) {
          todo.push_back(argv[i]);
      }

      // set up mapper thread vector;
      std::vector<std::thread> mapper_threads;
      for(int i = 0; i < num_mappers; i++) {
          mapper_threads.push_back(std::thread(map_worker, map, &todo, &todo_mutex));
      }

      // wait for all the workers to finish
      while (mapper_threads.size() > 0) {
          mapper_threads.pop().join();
      }

      std::vector<std::thread> reducer_threads;
      for (int i = 0; i < num_reducers; i++) {
          reducer_threads.push_back(std::thread(reduce_worker, reduce, i));
      }

      while (reducer_threads.size() > 0) {
          reducer_threads.pop().join();
      }

      delete processed_data; // do we also need to free all the strings in the map?
    }
}

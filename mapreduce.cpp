#include "mapreduce.hh"
#include <map>
#include <vector>
#include <mutex>
#include <utility>
#include <thread>
#include <memory>
#include <iostream>


// The p's are meant to stand for 'processed', because these types are used
// in the processed_data global variable.
using value_set_t = std::pair<std::vector<std::string>, std::shared_ptr<std::mutex> >;
using pdata_map_t = std::map<std::string, value_set_t>;
using pdata_partition_t = std::pair<pdata_map_t, std::shared_ptr<std::mutex> >;
using pdata_t = std::vector<pdata_partition_t>;

// processed_data is a pointer which points to the shared data structure that
//  all of the mappers add key-value pairs to as they work.  Partitioner is set
//  to the partition function passed by the user to MR_Run.  Both of these
//  variables are global because MR_Emit and get_next need to use them when
//  called by the user.

pdata_t* processed_data;
MapReduce::partitioner_t partitioner;

// *processed_data is a vector of partitions, each corresponding to one of the
//  buckets into which keys are divided by partitioner.  Each partition contains
//  a (ordered) map and a mutex, used to lock the map when adding new keys to it.
//  The map sends each key to a value_set, which is vector, mutex pair.  The
//  vector contains all the values that have been emitted for that key, while
//  the value_set mutex is used to lock the value_set vector to avoid data races
//  when adding new values.

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
void map_worker(MapReduce::mapper_t map, std::vector<char*>* todo, std::mutex* todo_mutex) {
    while (1) {
        todo_mutex->lock();
        if (todo->size() > 0) {
            char* task = todo->back();
            todo->pop_back();
            todo_mutex->unlock();
            map(task);
        } else {
            todo_mutex->unlock();
            break;
        }
    }
}

/*
    get_next is passed to the user reduce function, where it is used repeatedly
    to get all the values corresponding to a particular key.  It works by
    parsing *processed_data to get the vector of values associated with a given
    key, and then removing and returning the last element (if there is one) or
    returning the empty string (if there are no more).  At this stage, since
    there will only be one thread reducing in each partition, removing values
    as they're used is acceptable and requires no locking, so the mutexes in
    *processed_data are ignored.
*/
const std::string get_next(const std::string& key, int partition_number) {
    pdata_partition_t* partition = &(processed_data->at(partition_number)); //we dont check the bound here, but vector.at does. The error message at gives isnt very helpful.
    pdata_map_t* map = &(partition->first);
    std::vector<std::string>* values_of_key = &(map->at(key).first); // this is done by reference to ensure no copy is created
    if (values_of_key->size() > 0) {
        std::string rv = values_of_key->back();
        values_of_key->pop_back();
        return rv;
    } else {
        return "";
    }
}


/*
    This function is used by MR_Run to repeatedly call the user-defined reduce
    function on all the keys in a given partition.  This is done in order, using
    the ordering provided by the map container.
*/
void reduce_worker(MapReduce::reducer_t reduce, int partition_number) {
    // loop through the map at that partition
    // call reduce on each of the keys in order
    pdata_partition_t *partition = &(processed_data->at(partition_number)); //this is a fun trick we used a lot to make the types line up
    pdata_map_t kv_map = partition->first;
    for(std::map<std::string,value_set_t>::iterator iter = kv_map.begin(); iter != kv_map.end(); ++iter)
    {
        std::string k = iter->first;
        reduce(k, get_next, partition_number);
    }
}

namespace MapReduce {

    // called by Map - this adds a key, value pair to the global data structure

    /*
        The process works as follows:
            1. check if the key already exists in the right partition_map
                2. if it doesn't, lock the partition_mutex
                3. check that it's still missing
                4. initialize a new value_set at the key
            5. get the value_set associated with the key
            6. lock the associated mutex
            7. add the given value to the value_set
    */

    void MR_Emit(const std::string& key, const std::string& value) {
        // FIRST: figure out which partition this key belongs in, and get pointers
        //  to the appropriate map

        int partition_number = partitioner(key, processed_data->size());
        pdata_partition_t* partition = &(processed_data->at(partition_number));
        pdata_map_t* partition_map = &(partition->first);
        std::mutex* partition_mutex = partition->second.get();

        // check the map to see if it has an entry for the key
        // if not, lock the map (to prevent double entries) and add the key
        pdata_map_t::iterator key_it = partition_map->find(key);
        if (key_it == partition_map->end()) {
            partition_mutex->lock();
            key_it = partition_map->find(key);
            if (key_it == partition_map->end()) {
                partition_map->emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());//, value_set_t());//std::vector<std::string>(), std::mutex()));
                partition_map->at(key).second.reset(new std::mutex);
            }

            partition_mutex->unlock();
        }

        // once you get here, the map definitely has an entry for the key
        // now put the value on the end of the list, locking the list first
        // to prevent data races.

        value_set_t* value_set = &(partition_map->at(key));
        std::vector<std::string>* values = &(value_set->first);
        std::mutex* value_set_mutex = value_set->second.get();
        value_set_mutex->lock();
        values->push_back(value);
        value_set_mutex->unlock();
    }
    // we didnt mess with the logic, but we made it do the typing thing properly
    unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions) {
        unsigned long hash = 5381;
        int s = (int) key.size();
        for (int i = 0; i < s; i++){
            hash = hash * 33 + key[i];
        }
        return hash % num_partitions;
    }

    /*
        Called by the user to carry out the map-reduce process.
        Inputs:
            argc: the number of elements in argv
            argv: an array containing all the inputs to map (the first one is
                ignored)
            map: a user-defined function that takes one of the character
                pointers in argv and maps it, using MR_Emit to output the results.
            num_mappers: the number of threads that should be created to carry
                out mapping work.
            reduce: a user-defined function that takes (key, get_next, i) and
                reduces the values associated with that key, using get_next to
                get each value.
            num_reducers: both the number of partitions and the number of threads
                to create to do reducing work.  Each thread will reduce one partition.
            partition: a (possibly) user-defined function, which divides keys
                into partitions my mapping them to integers.  It takes the key
                and the total number of partitions.
        Output:
            Presumably reduce has a side-effect when called that produces the
                desired output.
        A brief outline of MR_Run:
            1. initialize processed_data and partitioner global variables
            2. set up a task vector and task mutex to coordinate mapper threads
            3. start the appropriate number of mapper threads and wait for them
                to finish
            5. start the appropriate number of reducer threads and wait for them
                to finish
            6. free used memory
    */

    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition) {
        processed_data = new pdata_t();
        partitioner = partition;
        // create num_mappers mapping threads
        // each mapper thread grabs a task from the vector, calls Map on it
        // then create num_reducers reducing threads
        // each reducer thread calls Reduce on the keys in its partition, in order

        // this creates the elements of the global data structure properly
        // some fun tricks to make sure the mutex exist because vector elements must be shareable and mutexes arent.
        for (int i = 0; i < num_reducers; i++) {
          processed_data->emplace_back();
          processed_data->back().second.reset(new std::mutex);
        }

        // set up task vector so that workers can find stuff to do (and lock while they get it)
        std::vector<char*> todo;
        std::mutex todo_mutex;
        for (int i = 1; i < argc; i++) {
            todo.push_back(argv[i]);
        }


        // create num_mappers map_workers, start them going
        std::vector<std::thread> mapper_threads;
        for(int i = 0; i < num_mappers; i++) {
          mapper_threads.push_back(std::thread(map_worker, map, &todo, &todo_mutex));
        }

        // wait for all the workers to finish
        while (mapper_threads.size() > 0) {
            mapper_threads.back().join();
            mapper_threads.pop_back();
        }

        // create num_reducers reducer workers, start them going
        std::vector<std::thread> reducer_threads;
        for (int i = 0; i < num_reducers; i++) {
          reducer_threads.push_back(std::thread(reduce_worker, reduce, i));
        }

        // wait for them to finish
        while (reducer_threads.size() > 0) {
          reducer_threads.back().join();
          reducer_threads.pop_back();
        }

        // all the data is here, so it all frees nicely. this makes it all worth it.
        delete processed_data;
    }
}

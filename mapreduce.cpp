#include "mapreduce.hh"
#include <map>
#include <vector>
#include <mutex>
#include <utility>
#include <thread>
#include <memory>
#include <iostream>


using value_set_t = std::pair<std::vector<std::string>, std::shared_ptr<std::mutex> >;
using pdata_map_t = std::map<std::string, value_set_t>;
using pdata_partition_t = std::pair<pdata_map_t, std::shared_ptr<std::mutex> >;
using pdata_t = std::vector<pdata_partition_t>;

pdata_t* processed_data;
MapReduce::partitioner_t partitioner;
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
    void MR_Emit(const std::string& key, const std::string& value) { // called by Map - this adds a key, value pair to the
        // FIRST: figure out which partition this key belongs in, and get pointers
        //  to the appropriate map

        int partition_number = partitioner(key, processed_data->size()); // this might be problematic
        pdata_partition_t* partition = &(processed_data->at(partition_number));
        pdata_map_t* partition_map = &(partition->first);
        std::mutex* partition_mutex = partition->second.get();

        // check the map to see if it has an entry for the key
        // if not, lock the map (to prevent double entries) and add the key
        pdata_map_t::iterator key_it = partition_map->find(key);
        if (key_it == partition_map->end()) {

            //std::cout << "ATTEMPTING TO CLAIM PARTITION LOCK\n";
            //std::cout << partition_mutex << "\n";
            partition_mutex->lock();
            key_it = partition_map->find(key);
            if (key_it == partition_map->end()) {
                partition_map->emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());//, value_set_t());//std::vector<std::string>(), std::mutex()));
                partition_map->at(key).second.reset(new std::mutex);
            }

            //std::cout << "RELEASING PARTITION LOCK\n";

            partition_mutex->unlock();
        }

        // once you get here, the map definitely has an entry for the key
        // now put the value on the end of the list

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

    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition) {
        processed_data = new pdata_t();
        partitioner = partition;
        // create num_mappers mapping threads
        // each mapper thread grabs a task from the vector, calls Map on it
        // then create num_reducers reducing threads
        // each reducer thread calls Reduce on the keys in its partition, in order
        // get_next needs to be an iterator

        // std::cout << "Setting up processed_data\n";

        // this creates the elements of the global data structure properly
        // some fun tricks to make sure the mutex exist because vector elements must be shareable and mutexes arent.
        for (int i = 0; i < num_reducers; i++) {
          processed_data->emplace_back();
          processed_data->back().second.reset(new std::mutex);
        }

        // std::cout << "Setting up task vector\n";
        // set up task vector so that workers can find stuff to do (and lock while they get it)
        std::vector<char*> todo;
        std::mutex todo_mutex;
        for (int i = 1; i < argc; i++) {
            todo.push_back(argv[i]);
        }


        // std::cout << "Setting up mapper_threads\n";
        // create num_mappers map_workers, start them going
        std::vector<std::thread> mapper_threads;
        for(int i = 0; i < num_mappers; i++) {
          mapper_threads.push_back(std::thread(map_worker, map, &todo, &todo_mutex));
        }

        // std::cout << "Waiting for mappers to finish\n";
        // wait for all the workers to finish
        while (mapper_threads.size() > 0) {
            mapper_threads.back().join();
            mapper_threads.pop_back();
        }

        // std::cout << "Setting up num_reducers\n";
        // create num_reducers reducer workers, start them going
        std::vector<std::thread> reducer_threads;
        for (int i = 0; i < num_reducers; i++) {
          reducer_threads.push_back(std::thread(reduce_worker, reduce, i));
        }

        // std::cout << "Waiting for reducer_threads to finish\n";
        // wait for them to finish
        while (reducer_threads.size() > 0) {
          reducer_threads.back().join();
          reducer_threads.pop_back();
        }
        // std::cout << "Done!\n";
        delete processed_data; // all the data is here, so it all frees nicely. this makes it all worth it.
    }
}

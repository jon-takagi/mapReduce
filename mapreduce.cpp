#include "mapreduce.hh"
#include <map>
#include <vector>

std::vector<std::map<std::string, std::vector<std::string> > >* processed_data;
// should be a vector of <mutex, map> pairs, where the maps send strings to <mutex, vector<string>> pairs
// each partition number gets a mutex and a bunch of key-value pairs
// hashpartition sends keys to numbers in the num_partitions
// each reducer gets a single key
namespace MapReduce {
    void MR_Emit(const std::string& key, const std::string& value) { // called by Map - this adds a key, value pair to some centralized
        // todo: all of this
        // put the key and value into processed_data
    }
    unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions) {
      unsigned long hash = 5381;
      int c;
      while ((c = *(key++)) != '\0')
          hash = hash * 33 + c;
      return hash % num_partitions;
    }

    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition) {
      processed_data = new std::vector<std::map<std::string, std::vector<std::string> > >();
      // create num_mappers mapping threads
      // each mapper thread grabs a task from the vector, calls Map on it
      // then create num_reducers reducing threads
      // each reducer thread calls Reduce on the keys in its partition, in order
      // get_next needs to be an iterator
      for (int i = 0; i < num_reducers; i++) { // hopefully this works to set things up
        processed_data->push_back(std::map<std::string, std::vector<std::string> >());
      }
      delete processed_data;
    }
}

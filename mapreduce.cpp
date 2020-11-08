#include "mapreduce.hh"
namespace MapReduce {
    void MR_Emit(const std::string& key, const std::string& value) {

    }
    unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions) {

    }
    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition) {

    }
}

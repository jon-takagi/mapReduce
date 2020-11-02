// C++ redefinition of mapreduce.h

#pragma once

#include <functional>
#include <string>

namespace MapReduce {

// Getters return "" when there are no more keys matching the key (not NULL)
using getter_t = std::function<const std::string(const std::string& key, int partition_number)>;
using mapper_t = std::function<void(const char* filename)>;
using reducer_t = std::function<void(const std::string& key, getter_t getter, int partition_number)>;
using partitioner_t = std::function<unsigned long(const std::string& key, int num_partitions)>;

void MR_Emit(const std::string& key, const std::string& value);

unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions);

void MR_Run(int argc, char* argv[],
            mapper_t map, int num_mappers,
            reducer_t reduce, int num_reducers,
            partitioner_t partition);

} // namespace

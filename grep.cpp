#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.hh"
#include <iostream>

std::string target;

void Map(const char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        std::string line_but_a_string(line);
        if (line_but_a_string.find(target) != std::string::npos) {
            MapReduce::MR_Emit(line_but_a_string, " ");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(const std::string& key, MapReduce::getter_t get_next, int partition_number) {
    std::cout << key;
}
int main(int argc, char *argv[]) {
    if (argc == 1) {
        std::cout << "usage: grep PATTERN FILE1 ..." << std::endl;
        return 0;
    }
    target = argv[1];
    MapReduce::MR_Run(argc-1, argv + 1, Map, 10, Reduce, 10, MapReduce::MR_DefaultHashPartition);
}

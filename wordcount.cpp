#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.hh"

void Map(const char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MapReduce::MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(const std::string& key, MapReduce::getter_t get_next, int partition_number) {
    int count = 0;
    std::basic_string<char> value = get_next(key, partition_number);
    while (!value.empty()) {
        count++;
        value = get_next(key, partition_number);
    }
    printf("%s %d\n", key, count);
}
int main(int argc, char *argv[]) {
    MapReduce::MR_Run(argc, argv, Map, 10, Reduce, 10, MapReduce::MR_DefaultHashPartition);
}

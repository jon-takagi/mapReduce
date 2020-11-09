all: wordcount.bin

mapreduce.o:
	g++ -o mapreduce.o mapreduce.cpp -Wall -Werror -pthread -O3 --std=c++11 -c
wordcount.o:
	g++ -o wordcount.o wordcount.cpp -Wall -Werror -pthread -O3 --std=c++11 -c
wordcount.bin: mapreduce.o wordcount.o
	g++ -o wordcount.bin wordcount.o mapreduce.o -Wall -Werror -pthread -O3 --std=c++11
grep.o:
	g++ -o grep.o grep.cpp -Wall -Werror -pthread -O3 --std=c++11 -c
grep.bin: mapreduce.o grep.o
	g++ -o grep.bin mapreduce.o grep.o -Wall -Werror -pthread -O3 --std=c++11
clean:
	rm *.bin
	rm *.o

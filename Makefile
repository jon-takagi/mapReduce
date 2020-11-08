wordcount:
	g++ -o wordcount.bin wordcount.cpp -Wall -Werror -pthread -O3 --std=c++11
clean:
	rm *.bin

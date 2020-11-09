CSCI 393, Fall 2020
Project 3 (mapreduce)
Robert McCaull, Jonathan Takagi

Files:
    anotherTestFile.txt, bible.txt, bible_words.txt, sample.txt, testFile.txt: These are all sample inputs to the two test programs.  
    gen.py, samplegen.py: programs used to create some of the sample inputs.  
    worcount.cpp, grep.cpp: the two test programs we used to test our implementation of mapreduce.  wordcount.cpp is an edited (to work in c++) copy of the book's wordcount test, while grep.cpp is our own test.  
    mapreduce.cpp: the body of our implementation.  
    mapreduce.hh: the header given in the assignment, unmodified.  It's included by the test programs so they can use our mapreduce functions.  
    Makefile: the makefile we used, of course!  compiles wordcount.bin and grep.bin.  

Documentation for our implementation of mapreduce can be found in comments in mapreduce.cpp.  

To use wordcount.bin, pass it a series of filenames corresponding to files to search.  wordcount.bin should then print out each of the words found in any of those files, followed by the number of times it was found in each file.  

To use grep.bin, pass it the following command-line arguments:
    1. a string 'target', with no spaces
    2. following 1, a series of filenames corresponding to files to search

grep.bin should then print each line in one of the targeted files which contains the target substring.  

We tested our code by compiling grep.bin and wordcount.bin, running them on sample inputs, and observing the output.  We also ran them in valgrind, and didn't see memory leaks.  

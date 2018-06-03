CC = g++
CCFLAGS= -Wextra -Wall -Wvla -std=c++11 -pthread -g -DNDEBUG
TARGETS = libMapReduceFramework

all: $(TARGETS)

# Library Compilation
libMapReduceFramework: MapReduceFramework.h MapReduceFramework.o SampleClient.o MapReduceClient.h
	ar rcs libMapReduceFramework.a MapReduceFramework.o SampleClient.o


# Object Files
MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
	$(CC) $(CCFLAGS) -c MapReduceFramework.cpp

SampleClient.o: SampleClient.cpp SampleClient.h MapReduceFramework.h
	$(CC) $(CCFLAGS) -c SampleClient.cpp

#tar
tar:
	tar -cf ex2.tar MapReduceFramework.cpp Makefile

.PHONY: clean

clean:
	-rm -f *.o libMapReduceFramework

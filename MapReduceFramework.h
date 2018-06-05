#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"
//// remove: todo
//#include <iostream>
//#include <vector>
//#include <pthread.h>
//#include <atomic>
//#include <algorithm>
//#include <functional>
//#include <semaphore.h>
//
////todo: remove
//class Barrier {
//public:
//	Barrier(int numThreads);
//	~Barrier();
//	void barrier();
//
//private:
//	pthread_mutex_t mutex;
//	pthread_cond_t cv;
//	int count;
//	int numThreads;
//};
//
//typedef struct ThreadCtx
//{
//	const InputVec* MapInputVector;
//	OutputVec* outputVec;
//	const MapReduceClient* client;
//	std::atomic<int>* atomicCounter;
//    int selfId;
//	bool* doneShuffling;
//	std::vector<IntermediateVec>* mapNSortOutput;
//	std::vector<IntermediateVec>* shuffleOutput;
//	Barrier* barrier;
//	pthread_mutex_t* reduceMutex;
//	pthread_mutex_t* outputVecMutex;
//	sem_t* semaphore;
//	IntermediateVec* localMapOutput;
//
//}ThreadCtx;
//////////////////

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);


void runMapReduceFramework(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel);

#endif //MAPREDUCEFRAMEWORK_H

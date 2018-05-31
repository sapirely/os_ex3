/**
 * @file MapReduceFramework.cpp
 *
 */

// ------------------------------ includes ----------------------------

#include <iostream>
#include <vector>
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <functional>
#include <semaphore.h>

using namespace std;
// ------------------------------- globals -----------------------------

// ---------------------------- Barrier class --------------------------

class Barrier {
public:
    Barrier(int numThreads);
    ~Barrier();
    void barrier();

private:
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    int count;
    int numThreads;
};

Barrier::Barrier(int numThreads) : mutex(PTHREAD_MUTEX_INITIALIZER),
                                   cv(PTHREAD_COND_INITIALIZER),
                                   count(0),
                                   numThreads(numThreads){}

Barrier::~Barrier()
{
    if (pthread_mutex_destroy(&mutex) != 0) {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
        exit(1);
    }
    if (pthread_cond_destroy(&cv) != 0){
        fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
        exit(1);
    }
}

void Barrier::barrier()
{
    if (pthread_mutex_lock(&mutex) != 0){
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
        exit(1);
    }
    if (++count < numThreads) {
        if (pthread_cond_wait(&cv, &mutex) != 0){
            fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
            exit(1);
        }
    } else {
        count = 0;
        if (pthread_cond_broadcast(&cv) != 0) {
            fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
            exit(1);
        }
    }
    if (pthread_mutex_unlock(&mutex) != 0) {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
        exit(1);
    }
}

// -------------------------- helper struct ------------------------------
/**
 * Gathers all the variables needed for threadsPart(), since we can pass only 1 arg (void*) in
 * pthread_create.
 */
struct ThreadContext {
    std::atomic<int>* atomic_counter;
    const InputVec* inputVec;
    const MapReduceClient* client;
    std::vector<IntermediatePair>* intermidiateVec;
    const int multiThreadLevel;
    Barrier* barrier;
    sem_t* semaphore;
    pthread_mutex_t* mutex; //
};

// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);

// ---------------------------- helper methods & structs --------------------------

// primary version
void shuffle(IntermediateVec* intermediateVec, ThreadContext* context){

    // gets sorted intermediary vectors
    // elements are popped from the back of each vector
    //todo:
    // use semaphore for counting vectors
    // whenever a new vector is inserted to the queue, call sem_post()
    // use mutex to protect access to the queue

    K2* currentKey = nullptr;
    // vector of vectors of pairs: key and value
    std::vector<std::vector<K2*, V2*>> vecQueue;
    // maybe shouldn't be declared here - how will it work with the threads?
    // todo

    // init first vector for first key
    sem_wait(context->semaphore);
    vecQueue.emplace_back(std::vector<K2*, V2*>());
    sem_post(context->semaphore);


    // elements are popped from the back of each vector
    while (!intermediateVec->empty())
    {
        // iterate over intermediate vec - insert all values with a certain key
        // in a sequence
        if (currentKey == nullptr) // first key
        {
            currentKey = intermediateVec->back().first;
        }
        else if (currentKey != intermediateVec->back().first) // new key
        {
            // prev key is done -> should change keys
            currentKey = intermediateVec->back().first;
            sem_wait(context->semaphore);
            vecQueue.emplace_back(std::vector<K2*, V2*>());
            sem_post(context->semaphore);
        }
        // now key is identical to the one at the back of the vector

        // enter value to the sequence (the vector of key CurrentKey)
        sem_wait(context->semaphore);
        vecQueue.back().emplace_back(intermediateVec->back());
        sem_post(context->semaphore);
        intermediateVec->pop_back();

    }


    // todo - from instructions:
    // difficult to split efficiently into parallel threads - so we
    // run parallel with Reduce
}

// ------------------------------- todo's -----------------------------
// make sure all sys calls are ok


// ---------------------------- library methods -------------------------

/**
 * Called in Map phase
 */
void emit2 (K2* key, V2* value, void* context){

}

/**
 * Called in Reduce phase
 */
void emit3 (K3* key, V3* value, void* context){

}

/**
 * Performs the actual algorithm.
 */
void* threadsPart(void* arg)
{
    auto threadCtx = (ThreadContext*) arg;
    int old_value;

    // call map:
    K1* key;
    V1* val;
    std::vector<IntermediatePair>* localIntermediateVec = {};
    while (*threadCtx->atomic_counter <= threadCtx->inputVec->size())
        //todo: what does it mean, that a thread that has finished a job returns to the pool?
        // todo: use pthread_cond_wait instead of the while?
    {
        old_value = (*(threadCtx->atomic_counter))++;
        key = (*threadCtx->inputVec)[old_value].first;
        val = (*threadCtx->inputVec)[old_value].second;
        (*threadCtx->client).map(key, val, localIntermediateVec);
    }

    std::sort((*localIntermediateVec).begin(), (*localIntermediateVec).end()); // todo: catch exceptions.

    if (!pthread_self()) // if it's the main thread (0)
    {
        shuffle(*threadCtx->intermidiateVec);
    }

    threadCtx->barrier->barrier();

    // reduce:
    // todo: wait on the semaphor


}


/**
 * Runs the MapReduce algorithm.
 * @param client
 * @param inputVec - vector of (k1,v1) pairs
 * @param outputVec - vector of (k3,v3) pairs
 * @param multiThreadLevel - number of threads to be created
 */
void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){

    // initialize context (containers pointers, etc.) for all threads:
    pthread_t threads[multiThreadLevel];
    std::atomic<int> atomic_pairs_counter(0);
    sem_t sem;
    int ret = sem_init(&sem, 0, 1); // todo: check args + ret value
    auto mutex = PTHREAD_MUTEX_INITIALIZER;


    ThreadContext threadCtx = {&atomic_pairs_counter, &inputVec, &client, nullptr,
                               multiThreadLevel, new Barrier(multiThreadLevel), &sem, &mutex}; //todo: intermediateVec!!!


    // create threads & atomic counter:
    for (int i = 0; i < multiThreadLevel; i++)
    {
        ret = pthread_create(&threads[i], nullptr, threadsPart, &threadCtx);
        if (!ret)
        {
            // ERROR, do something.
        }
    }
    threadsPart(nullptr);


}

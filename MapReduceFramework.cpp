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
    const InputVec* inputVec;
    const MapReduceClient* client;
    std::atomic<int>* atomic_counter;
    const int multiThreadLevel;
    pthread_t mainThreadId;
    bool* doneShuffling;
    std::vector<IntermediateVec>* intermediateVecQueue;
    Barrier* barrier;
    pthread_mutex_t* vecQueueMutex;
    pthread_mutex_t* IntermediateVecMutex;
    sem_t* semaphore;
};

// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);

// ---------------------------- helper methods --------------------------

// primary version
// updates vector of vectors of pairs: key and value
void shuffle(IntermediateVec* inputVec, ThreadContext* context){

    // gets sorted intermediary vectors
    // elements are popped from the back of each vector
    //todo:
    // use semaphore for counting vectors
    // whenever a new vector is inserted to the queue, call sem_post()
    // use mutex to protect access to the queue

    K2* currentKey = nullptr;

    // init first vector for first key
    sem_wait(context->semaphore);
    context->intermediateVecQueue->emplace_back(std::vector<K2*, V2*>());
    sem_post(context->semaphore);

    // elements are popped from the back of each vector
    while (!inputVec->empty())
    {
        // iterate over intermediate vec - insert all values with a certain key
        // in a sequence
        if (currentKey == nullptr) // first key
        {
            currentKey = inputVec->back().first;
        }
        else if (currentKey != inputVec->back().first) // new key
        {
            // prev key is done -> should change keys
            currentKey = inputVec->back().first;
            sem_wait(context->semaphore);
            context->intermediateVecQueue->emplace_back(std::vector<K2*, V2*>());
            sem_post(context->semaphore);
        }
        // now key is identical to the one at the back of the vector

        // enter value to the sequence (the vector of key CurrentKey)
        sem_wait(context->semaphore);
        context->intermediateVecQueue->back().emplace_back(inputVec->back());
        sem_post(context->semaphore);
        inputVec->pop_back();

    }
    // todo - from instructions:
    // difficult to split efficiently into parallel threads - so we
    // run parallel with Reduce
}

void reduce(ThreadContext* context){
    // calls client func reduce for each vector in the vector queue
    for (int i=0; i<context->intermediateVecQueue->size(); i++)
    {
        sem_wait(context->semaphore);
        context->client->reduce(&(context->intermediateVecQueue->at(i)), context);
        sem_post(context->semaphore);
    }

}

/**
 * Performs cleanups and exits with exitCode.
 */
void exitLib(ThreadContext* threadCtx, int exitCode)
{
    delete threadCtx->barrier;
    sem_destroy(threadCtx->semaphore);
    pthread_mutex_destroy(threadCtx->IntermediateVecMutex);
    pthread_mutex_destroy(threadCtx->vecQueueMutex);
    exit(exitCode);
}

// ------------------------------- todo's -----------------------------



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
    IntermediateVec localIntermediateVec = {};
    while (*threadCtx->atomic_counter <= threadCtx->inputVec->size())
    {
        old_value = (*(threadCtx->atomic_counter))++;
        key = (*threadCtx->inputVec)[old_value].first;
        val = (*threadCtx->inputVec)[old_value].second;
        (*threadCtx->client).map(key, val, &localIntermediateVec);
    }

    // sort:
    try {
        std::sort((localIntermediateVec).begin(), (localIntermediateVec).end());
    }
    catch (const std::bad_alloc& e) {
        std::cerr << "An error has occurred while sorting." << std::endl;
        exitLib(threadCtx, 1);
    }

    if (pthread_self() == threadCtx->mainThreadId) // if it's the main thread, start shuffling.
    {
        shuffle(&localIntermediateVec, threadCtx);
        *(threadCtx->doneShuffling) = true;
    }
    threadCtx->barrier->barrier();

    // reduce:
    while (!(threadCtx->doneShuffling && (*threadCtx->intermediateVecQueue).empty()))
    {
        sem_wait(threadCtx->semaphore);
        pthread_mutex_lock(threadCtx->IntermediateVecMutex);
        IntermediateVec* pair = &((*threadCtx->intermediateVecQueue).back()); // pair is a key (char)
        // and a vector intermediateVec - counts from all threads of the key char.
        (*threadCtx->intermediateVecQueue).pop_back();
        pthread_mutex_unlock(threadCtx->IntermediateVecMutex);
        threadCtx->client->reduce(pair, &threadCtx);
    }

    // is something supposed to happen when the thread is done?

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

    // initialize context's variables & containers:
    ThreadContext threadCtx;
    pthread_t threads[multiThreadLevel - 1];
    std::atomic<int> atomic_pairs_counter(0);
    pthread_t mainThreadId = pthread_self();
    bool doneShuffling = false;
    std::vector<IntermediateVec>* intermediateVecQueue = {};
    std::vector<IntermediateVec> intermediateVec = {};

    // initialize barrier, mutexes & semaphore:
    auto barrier = new Barrier(multiThreadLevel);
    auto vecQueueMutex = PTHREAD_MUTEX_INITIALIZER;
    auto IntermediateVecMutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t sem;
    int ret = sem_init(&sem, 0, 1);
    if (ret != 0)
    {
        fprintf(stderr, "An error has occurred while initializing the semaphore.");
        exitLib(&threadCtx, 1);
    }

    // create context for all threads:
     threadCtx = {&inputVec, &client, &atomic_pairs_counter, multiThreadLevel,
                               mainThreadId, &doneShuffling, intermediateVecQueue, barrier,
                               &vecQueueMutex, &IntermediateVecMutex, &sem};

    // create threads:
    for (int i = 0; i < multiThreadLevel - 1; i++)
    {
        ret = pthread_create(&threads[i], nullptr, threadsPart, &threadCtx);
        if (!ret)
        {
            fprintf(stderr, "An error has occurred while creating a thread.");
            exitLib(&threadCtx, 1);
        }
    }
    threadsPart(nullptr);

    exitLib(&threadCtx, 0);

}

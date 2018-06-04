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

// -------------------------- struct GeneralContext------------------------------
//

typedef struct GeneralContext
{
    const InputVec* MapInputVector;
    OutputVec* outputVec;
    const MapReduceClient* client;
    std::atomic<int>* atomicCounter;
    pthread_t mainThreadId;
    bool* doneShuffling;
    std::vector<IntermediateVec>* mapNSortOutput;
    std::vector<IntermediateVec>* shuffleOutput;
    Barrier* barrier;
    pthread_mutex_t* reduceMutex;
    pthread_mutex_t* outputVecMutex;
    sem_t* semaphore;
} GeneralContext;

// -------------------------- struct ThreadCtx ------------------------------
typedef struct ThreadCtx
{
    GeneralContext* generalCtx;
    IntermediateVec* localMapOutput;
}ThreadCtx;



// -------------------------- helper struct ------------------------------
/**
 * Gathers all the variables needed for threadsPart(), since we can pass only 1 arg (void*) in
 * pthread_create.
 */
//typedef struct GeneralContext {
//    const InputVec* inputVec;
//    OutputVec* outputVec;
//    const MapReduceClient* client;
//    std::atomic<int>* atomic_counter;
//    pthread_t mainThreadId;
//    bool* doneShuffling;
//    std::vector<IntermediateVec>* intermediateVecQueue;
//    Barrier* barrier;
//    pthread_mutex_t* vecQueueMutex;
//    pthread_mutex_t* intermediateVecMutex;
//    pthread_mutex_t* outputVecMutex;
//    sem_t* semaphore;
//} GeneralContext;


//typedef struct ThreadContext {
//    GeneralContext* generalCtx = nullptr;
//    IntermediateVec intermediatePairs = nullptr;
//} ThreadContext;

// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);
bool areKeysEqual(K2* key1, K2* key2);
void shuffle(GeneralContext* genCtx);
void exitLib(ThreadCtx* threadCtx, int exitCode);

// ---------------------------- helper methods --------------------------

/**
 * Checks if two K2* keys are equal (since they have a custom operator)+.
 * @return True if equal, false otherwise.
 */
bool areKeysEqual(K2* key1, K2* key2){
    if ((key1>key2) || (key1<key2))
    {
        return false;
    }
    return true;
}

/**
 * Create vector of vectors with the same key.
 * @param genCtx - general context
 */
void shuffle(GeneralContext* genCtx){
    std::vector<IntermediateVec> shuffledVectors;
    IntermediateVec currentVector;
    K2* maxKey = genCtx->mapNSortOutput->at(0).back().first;

    K2* nextKey = maxKey;
    // find min key
    for (auto vector : *(genCtx->mapNSortOutput))
    {
        if (maxKey < vector.back().first)
        {
            maxKey = vector.back().first;
        }
    }

    int numOfVectors = (int) genCtx->mapNSortOutput->size();

    while (!genCtx->mapNSortOutput->empty())
    {
        currentVector = {};
        // add all pairs with maxKey to currentVector
        for (int j = 0; j < numOfVectors; j++)
        {
            // pop all vectors with key maxKey into currentVector
            while (areKeysEqual(maxKey,genCtx->mapNSortOutput->at(j).back()
                    .first))
            {
                currentVector.emplace_back(
                        genCtx->mapNSortOutput->at(j).back());
                genCtx->mapNSortOutput->at(j).pop_back();
            }
            // if the next key in the current vector is bigger than nextKey,
            // put it in nextKey
            if (nextKey < genCtx->mapNSortOutput->at(j).back().first)
            {
                nextKey = genCtx->mapNSortOutput->at(j).back().first;
            }
        }
        pthread_mutex_lock(genCtx->reduceMutex);
        genCtx->shuffleOutput->push_back(currentVector);
        pthread_mutex_unlock(genCtx->reduceMutex);
    }
}

//void reduce(GeneralContext* context){
//    // calls client func reduce for each vector in the vector queue
//    for (int i=0; i<context->mapNSortOutput->size(); i++)
//    {
//        sem_wait(context->semaphore);
//        context->client->reduce(&(context->mapNSortOutput->at(i)), context);
//        sem_post(context->semaphore);
//    }
//
//}

/**
 * Performs cleanups and exits with exitCode.
 */
void exitLib(ThreadCtx* threadCtx, int exitCode)
{
    delete threadCtx->generalCtx->barrier;
    sem_destroy(threadCtx->generalCtx->semaphore);
    pthread_mutex_destroy(threadCtx->generalCtx->reduceMutex);
    pthread_mutex_destroy(threadCtx->generalCtx->outputVecMutex);
    exit(exitCode);
}

// ------------------------------- todo's -----------------------------



// ---------------------------- library methods -------------------------

/**
 * Called in Map phase
 */
void emit2 (K2* key, V2* value, void* context){
    auto tc = (ThreadCtx*) context;
    tc->localMapOutput->emplace_back(make_pair(key,value)); //todo

}

/**
 * Called in Reduce phase
 */
void emit3 (K3* key, V3* value, void* context){
    auto tc = (GeneralContext*) context;
    if (pthread_mutex_lock(tc->outputVecMutex) != 0){
        fprintf(stderr, "[[Output]] error on pthread_mutex_lock");
        exit(1);
    }
    tc->outputVec->push_back(make_pair(key,value));
    if (pthread_mutex_unlock(tc->outputVecMutex) != 0) {
        fprintf(stderr, "[[Output]] error on pthread_mutex_unlock");
        exit(1);
    }
}

/**
 * Performs the actual algorithm.
 */
void* threadsPart(void* arg)
{
    auto threadCtx = (ThreadCtx*) arg;
    int old_value;

    // call map on MapInputVector's elements:
    K1* key;
    V1* val;
    while ((threadCtx->generalCtx->atomicCounter->load()) < threadCtx->generalCtx->MapInputVector->size())
    {
        old_value = (*(threadCtx->generalCtx->atomicCounter))++;
        key = (*threadCtx->generalCtx->MapInputVector)[old_value].first;
        val = (*threadCtx->generalCtx->MapInputVector)[old_value].second;
        (*threadCtx->generalCtx->client).map(key, val, &threadCtx);

        // sort the resulting vector of pairs and push it to the queue of intermediate vectors:
        try {
            std::sort((*threadCtx->localMapOutput).begin(), (*threadCtx->localMapOutput).end());
            threadCtx->generalCtx->mapNSortOutput->push_back((*threadCtx->localMapOutput));
        }
        catch (const std::bad_alloc& e) {
            std::cerr << "An error has occurred while sorting." << std::endl;
            exitLib(threadCtx, 1);
        }
    }

    // wait for all the other threads to finish map&sort:
    threadCtx->generalCtx->barrier->barrier();

    // if it's the main thread, start shuffling.
    if (pthread_self() == threadCtx->generalCtx->mainThreadId)
    {
        shuffle(threadCtx->generalCtx);
        *(threadCtx->generalCtx->doneShuffling) = true;
    }


    // reduce:
    IntermediateVec sameKeyedpairs;
    while (!(threadCtx->generalCtx->doneShuffling &&
            (*threadCtx->generalCtx->mapNSortOutput).empty()))
    {
        // pop the last vector of same keyed pairs from the intermediate-vectors vector:
        sem_wait(threadCtx->generalCtx->semaphore);
        pthread_mutex_lock(threadCtx->generalCtx->reduceMutex);
        sameKeyedpairs = (*threadCtx->generalCtx->shuffleOutput).back();
        (*threadCtx->generalCtx->shuffleOutput).pop_back();
        pthread_mutex_unlock(threadCtx->generalCtx->reduceMutex);
        // call reduce on it:
        threadCtx->generalCtx->client->reduce(&sameKeyedpairs, &threadCtx);
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
    pthread_t threads[multiThreadLevel - 1];
    std::atomic<int> atomic_pairs_counter(0);
    pthread_t mainThreadId = pthread_self();
    bool doneShuffling = false;
    std::vector<IntermediateVec> mapNsortOutput = {};
    std::vector<IntermediateVec> shuffleOutput = {};

    // initialize barrier, mutexes & semaphore:
    auto barrier = new Barrier(multiThreadLevel);
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t outputVecMutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t sem;
    int ret = sem_init(&sem, 0, 1);
    if (ret != 0)
    {
        fprintf(stderr, "An error has occurred while initializing the semaphore.");
        sem_destroy(&sem);
        pthread_mutex_destroy(&reduceMutex);
        pthread_mutex_destroy(&outputVecMutex);
        exit(1);
//        exitLib(&threadsCtxs[0], 1);
    }

    // create the general context and the local context for every thread:
//    auto generalContext = new GeneralContext(&inputVec, &outputVec, &client,
//                                          &atomic_pairs_counter,
//                                             mainThreadId, &doneShuffling, mapNsortOutput,
//                                             shuffleOutput, barrier, &reduceMutex, &outputVecMutex,
//                                             &sem);
    GeneralContext generalCtx = {&inputVec, &outputVec, &client, &atomic_pairs_counter, mainThreadId,
                                 &doneShuffling, &mapNsortOutput, &shuffleOutput, barrier, &reduceMutex,
                                 &outputVecMutex, &sem};
    ThreadCtx threadsCtxs[multiThreadLevel];
//    std::vector<IntermediateVec*> mapOutputs;

    // create threads:
    for (int i = 0; i < multiThreadLevel - 1; i++)
    {
        IntermediateVec localMapOutput = {};
        threadsCtxs[i].localMapOutput = &localMapOutput;
        threadsCtxs[i].generalCtx = &generalCtx;
        ret = pthread_create(&threads[i], nullptr, threadsPart, &threadsCtxs[i]);
        if (!ret)
        {
            fprintf(stderr, "An error has occurred while creating a thread.");
            exitLib(&threadsCtxs[i], 1);
        }
    }
    threadsPart(&threadsCtxs[0]);
    exitLib(&threadsCtxs[0], 0);
}

//GeneralContext(const InputVec* inputVec, OutputVec* outputVec,
//               const MapReduceClient* client, std::atomic<int>* atomicCounter,
//               pthread_t mainThreadId, bool* doneShuffling,
//               std::vector<IntermediateVec>* mapNSortOutput,
//               std::vector<IntermediateVec>* shuffleOutput,
//               Barrier* barrier, pthread_mutex_t* reduceMutex,
//               pthread_mutex_t* outputVecMutex, sem_t* semaphore):
//
//        MapInputVector(inputVec), outputVec(outputVec), client(client),
//        atomicCounter(atomicCounter), mainThreadId(mainThreadId),
//        doneShuffling(doneShuffling), mapNSortOutput(mapNSortOutput),
//        shuffleOutput(shuffleOutput), barrier(barrier),
//        reduceMutex(reduceMutex), outputVecMutex(outputVecMutex),
//        semaphore(semaphore){}

//GeneralContext* generalCtx;
//IntermediateVec* localMapOutput;

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
#include "Barrier.h"

using namespace std;


// -------------------------- struct ThreadCtx ------------------------------
typedef struct ThreadCtx
{
    const InputVec* MapInputVector;
    OutputVec* outputVec;
    const MapReduceClient* client;
    std::atomic<int>* atomicCounter;
    int selfId;
    bool* doneShuffling;
    std::vector<IntermediateVec>* mapNSortOutput;
    std::vector<IntermediateVec>* shuffleOutput;
    Barrier* barrier;
    pthread_mutex_t* reduceMutex;
    pthread_mutex_t* outputVecMutex;
    sem_t* semaphore;
    IntermediateVec* localMapOutput;
}ThreadCtx;


// --------------------- struct Comparator for sort --------------------

/**
 * Comparator (used in std::sort).
 */
typedef struct comparePairs {
    bool operator()(const std::pair<K2*,V2*>& lhs, const std::pair<K2*,V2*>&
    rhs) const
    {
        return lhs.first < rhs.first;
    }
} comparePairs;


// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);
bool areKeysEqual(K2* key1, K2* key2);
void shuffle(ThreadCtx* genCtx);
void exitLib(ThreadCtx* threadCtx, int exitCode);


// ---------------------------- helper methods --------------------------



/**
 * Checks if two K2* keys are equal (since they have a custom operator)+.
 * @return True if equal, false otherwise.
 */
bool areKeysEqual(K2* key1, K2* key2){
    if ((key2<key1) || (key1<key2)) // todo - derefence
    {
        return false;
    }
    return true;
}

/**
 * Create vector of vectors with the same key.
 * @param genCtx - general context
 */
void shuffle(ThreadCtx* genCtx){
    cerr << "Thread 0: started shuffle" << endl;
    IntermediateVec currentVector;
    K2* maxKey = genCtx->mapNSortOutput->at(0).back().first;
    K2* nextKey;

    // find first max key:
    for (auto vector : *(genCtx->mapNSortOutput))
    {
        if (maxKey < vector.back().first)
        {
            maxKey = vector.back().first;
        }
    }

    int numOfVectors;
    while (!(genCtx->mapNSortOutput->empty()))
    {
        numOfVectors = (int) genCtx->mapNSortOutput->size();
        // init nextKey to any key other than max key
        for (int j = 0; j < numOfVectors; j++){
            if (!(areKeysEqual(maxKey, genCtx->mapNSortOutput->at(j).back().first)))
            {
                nextKey = genCtx->mapNSortOutput->at(j).back().first;
            }
        }
        currentVector = {};
        // add all pairs with maxKey to currentVector
        for (int j = 0; j < numOfVectors; j++)
        {
            // pop all vectors with key maxKey into currentVector
            while ((!genCtx->mapNSortOutput->at(j).empty()) && areKeysEqual(maxKey, genCtx->mapNSortOutput->at(j).back().first))
            {
                currentVector.push_back((genCtx->mapNSortOutput->at(j).back()));
                genCtx->mapNSortOutput->at(j).pop_back();
            }
            if (genCtx->mapNSortOutput->at(j).empty()){
                genCtx->mapNSortOutput->erase(genCtx->mapNSortOutput->begin()
                                              +j);
            } else
            {
                // if the next key in the current vector is bigger than nextKey,
                // put it in nextKey
                if (nextKey < genCtx->mapNSortOutput->at(j).back().first)
                {
                    nextKey = genCtx->mapNSortOutput->at(j).back().first;
                }
            }
        }
        pthread_mutex_lock(genCtx->reduceMutex);
        cerr << "Thread 0: locked reduceMutex" << endl;
        genCtx->shuffleOutput->push_back(currentVector);
        cerr << "Thread 0: pushed shuffle output " << endl;
        pthread_mutex_unlock(genCtx->reduceMutex);
        cerr << "Thread 0: released  reduceMutex" << endl;
        sem_post(genCtx->semaphore);
        maxKey = nextKey;
    }
}


/**
 * Performs cleanups and exits with exitCode.
 */
void exitLib(ThreadCtx* threadCtx, int exitCode)
{
    delete threadCtx->barrier;
    sem_destroy(threadCtx->semaphore);
    pthread_mutex_destroy(threadCtx->reduceMutex);
    pthread_mutex_destroy(threadCtx->outputVecMutex);
    exit(exitCode);
}


// ------------------------------- todo's -----------------------------
//check sys calls
// call destructors / free allocated memory?


// ---------------------------- library methods -------------------------

/**
 * Called in Map phase
 */
void emit2 (K2* key, V2* value, void* context){
    auto tc = (ThreadCtx*) context;
    tc->localMapOutput->push_back(make_pair(key,value)); //todo

}

/**
 * Called in Reduce phase
 */
void emit3 (K3* key, V3* value, void* context){
    auto tc = (ThreadCtx*) context;
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

    // call map on MapInputVector's elements:
    const InputVec input = (*threadCtx->MapInputVector);
    int old_value = (*(threadCtx->atomicCounter))++;
    while ((unsigned int) old_value < input.size())
    {
        threadCtx->client->map(input.at((unsigned int)old_value).first,
                               input.at((unsigned int)old_value).second, threadCtx);
        old_value = (*(threadCtx->atomicCounter))++;

        // sort the resulting vector of pairs and push it to the queue of intermediate vectors:
        try
        {
            std::sort(threadCtx->localMapOutput->begin(),
                      threadCtx->localMapOutput->end(), comparePairs());
            threadCtx->mapNSortOutput->push_back(*(threadCtx->localMapOutput));
        }
        catch (const std::bad_alloc &e)
        {
            std::cerr << "An error has occurred while sorting." << std::endl;
            exitLib(threadCtx, 1);
        }
    }

    // wait for all the other threads to finish map&sort:
    threadCtx->barrier->barrier();

    // if it's the main thread, start shuffling.
    if (threadCtx->selfId == 0)
    {
        shuffle(threadCtx);
        *(threadCtx->doneShuffling) = true;
    }

    // reduce phase:
    IntermediateVec sameKeyedpairs;
    while (!(threadCtx->doneShuffling && threadCtx->mapNSortOutput->empty()))
    {
        cerr << "waiting on semaphore: " << threadCtx->selfId << endl;
        sem_wait(threadCtx->semaphore);
        if (!(threadCtx->shuffleOutput->empty()))
        {
            pthread_mutex_lock(threadCtx->reduceMutex);
            cerr << threadCtx->selfId<< ": locked reduceMutex" << endl;
            cerr << threadCtx->selfId << ": shuffle output size: " << threadCtx->shuffleOutput->size() << endl;
            sameKeyedpairs = threadCtx->shuffleOutput->back();
            (*threadCtx->shuffleOutput).pop_back();
            cerr << threadCtx->selfId<< ": shuffle output size: " << threadCtx->shuffleOutput->size() << endl;
            pthread_mutex_unlock(threadCtx->reduceMutex);
            cerr << threadCtx->selfId << ": released reduceMutex" << endl;
            // call reduce on it:
            threadCtx->client->reduce(&sameKeyedpairs, threadCtx);
            cerr << threadCtx->selfId << ": back from reduce" << endl;

        }
    }

    // is something supposed to happen when the thread is done? free vector!

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
    pthread_t threads[multiThreadLevel];
    std::atomic<int> atomic_pairs_counter(0);
    bool doneShuffling = false;
    std::vector<IntermediateVec> mapNsortOutput = {};
    std::vector<IntermediateVec> shuffleOutput = {};

    // initialize barrier, mutexes & semaphore:
    Barrier barrier(multiThreadLevel);
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t outputVecMutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t sem;
    int ret = sem_init(&sem, 0, 0);
    if (ret != 0)
    {
        fprintf(stderr, "An error has occurred while initializing the semaphore.");
        sem_destroy(&sem);
        pthread_mutex_destroy(&reduceMutex);
        pthread_mutex_destroy(&outputVecMutex);
        exit(1);
    }
    ThreadCtx threadsCtxs[multiThreadLevel]; // all threads contexts
    // initialize context for thread 0:
    vector<IntermediateVec> localMapOutput((unsigned int)multiThreadLevel, IntermediateVec(0));
    threadsCtxs[0]= {&inputVec, &outputVec, &client, &atomic_pairs_counter, 0, &doneShuffling,
                     &mapNsortOutput, &shuffleOutput, &barrier, &reduceMutex, &outputVecMutex, &sem,
                     &(localMapOutput.at(0))};
    // todo: set values instead of lidros

    // create all threads & initialize their contexts:
    for (int i = 1; i < multiThreadLevel; i++)
    {
        threadsCtxs[i]= {&inputVec, &outputVec, &client, &atomic_pairs_counter, i, &doneShuffling,
                         &mapNsortOutput, &shuffleOutput, &barrier, &reduceMutex, &outputVecMutex,
                         &sem, &(localMapOutput.at(i))};
        // todo: set values instead of lidros
        ret = pthread_create(&threads[i], nullptr, threadsPart, &threadsCtxs[i]);
        if (ret)
        {
            fprintf(stderr, "An error has occurred while creating a thread.");
            exitLib(&threadsCtxs[i], 1);
        }
    }
    threadsPart(&threadsCtxs[0]);
    exitLib(&threadsCtxs[0], 0);
}
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
    int multiThreadLevel;
    bool* doneShuffling;
    std::vector<IntermediateVec>* mapNSortOutput;
    std::vector<IntermediateVec>* shuffleOutput;
    Barrier* barrier;
    pthread_mutex_t* reduceMutex;
    pthread_mutex_t* mapOutputMutex;
    pthread_mutex_t* outputVecMutex;
    sem_t* semaphore;
    IntermediateVec* localMapOutput;
}ThreadCtx;


// --------------------- struct Comparator for sort --------------------

/**
 * Comparator (used in std::sort).
 */
typedef struct comparePairs {
    bool operator()(const IntermediatePair& lhs, const IntermediatePair&
    rhs) const
    {
        return (*lhs.first) < (*rhs.first);
    }
} comparePairs;


// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);
bool areKeysEqual(const K2* key1, const K2* key2);
void shuffle(ThreadCtx* threadCtx);
void exitLib(ThreadCtx* threadCtx, int exitCode);


// ---------------------------- helper methods --------------------------



/**
 * Checks if two K2* keys are equal (since they have a custom operator)+.
 * @return True if equal, false otherwise.
 */
bool areKeysEqual(const K2* key1, const K2* key2)
{
    if ((*key2 < *key1) || (*key1 < *key2)) // todo - derefence
    {
        return false;
    }
    return true;
}

/**
 * Finds first maximun pair of all the last pairs of all vectors in Map's output.
 * @param ctx
 * @return
 */

K2* findMax(ThreadCtx *ctx)
{
    K2* maxKey = ctx->mapNSortOutput->at(0).back().first;
    for (auto vector : *(ctx->mapNSortOutput))
    {
        if ((!vector.empty()) && (maxKey < vector.back().first)) // todo: I added a patch - still can't
            // understand why are there empty vectors in mapNSortOutput.
        {
            maxKey = vector.back().first;
        }
    }
    return maxKey;
}



/**
 * Create vector of vectors with the same key.
 * @param threadCtx - general context
 */
void shuffle(ThreadCtx* threadCtx){
    IntermediateVec currentVector;
    K2* maxKey;

    while (!(*threadCtx->doneShuffling))
    {
        maxKey = findMax(threadCtx);

        // group all pairs with maxKey in currentVector:
        currentVector = {};
        IntermediatePair* p;
        for (int j = 0; j < threadCtx->mapNSortOutput->size(); j++)
        {
            // pop all vectors with key maxKey into currentVector
            while ((!threadCtx->mapNSortOutput->empty()) &&
                   (!threadCtx->mapNSortOutput->at(j).empty()) &&
                   (areKeysEqual(maxKey, threadCtx->mapNSortOutput->at(j).back().first)))
            {
                p = &(threadCtx->mapNSortOutput->at(j).back());
                currentVector.push_back(*p);
                threadCtx->mapNSortOutput->at(j).pop_back();
            }
            if (threadCtx->mapNSortOutput->at(j).empty())
            {
                threadCtx->mapNSortOutput->erase(threadCtx->mapNSortOutput->begin() + j);
                j-= 1; // todo so that we'll get over the next vector, which will get this input. IS THAT OK??
                if (threadCtx->mapNSortOutput->empty())
                {
                    *(threadCtx->doneShuffling) = true;
                    break;
                }
            }
        }

        // push current vector to buffer:
        if (pthread_mutex_lock(threadCtx->reduceMutex) != 0)
        {
            std::cerr << "[[Shuffle]] error on pthread_mutex_lock" << std::endl;
            exit(1);
        }
        threadCtx->shuffleOutput->push_back(currentVector);
        if (pthread_mutex_unlock(threadCtx->reduceMutex) != 0)
        {
            std::cerr << "[[Shuffle]] error on pthread_mutex_unlock" << std::endl;
            exit(1);
        }
        if (sem_post(threadCtx->semaphore) != 0)
        {
            std::cerr << "[[Shuffle]] error on sem_post" << std::endl;
            exit(1);
        }
    }
    // frees all threads waiting on the semaphore
    for (int i=0; i<threadCtx->multiThreadLevel; i++){
        if (sem_post(threadCtx->semaphore) != 0)
        {
            std::cerr << "[[Shuffle]] error on sem_post" << std::endl;
            exit(1);
        }
    }
}


/**
 * Performs cleanups and exits with exitCode.
 */
void exitLib(ThreadCtx* threadCtx)
{
    if (sem_destroy(threadCtx->semaphore) != 0)
    {
        std::cerr << "Error in sem_destroy" << std::endl;
        exit(1);
    }
    if (pthread_mutex_destroy(threadCtx->reduceMutex))
    {
        std::cerr << "Error in pthread_mutex_destroy" << std::endl;
        exit(1);

    }
    if (pthread_mutex_destroy(threadCtx->outputVecMutex))
    {
        std::cerr << "Error in pthread_mutex_destroy" << std::endl;
        exit(1);
    }
}


// ------------------------------- todo's -----------------------------
//check sys calls
// call destructors / free allocated memory?
// error handling - i.e. for mutexes


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
        std::cerr << "[[Output]] error on pthread_mutex_lock" << std::endl;
        exit(1);
    }
    tc->outputVec->push_back(make_pair(key,value));
    if (pthread_mutex_unlock(tc->outputVecMutex) != 0) {
        std::cerr << "[[Output]] error on pthread_mutex_unlock" << std::endl;
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
    while ((unsigned int) old_value < input.size()) // todo: what if context switch happens before this
    {
        threadCtx->client->map(input.at((unsigned int)old_value).first,
                               input.at((unsigned int)old_value).second, threadCtx);
        old_value = (*(threadCtx->atomicCounter))++;
    }

    // sort the resulting vector of pairs and push it to the queue of intermediate vectors:
    try
    {
        std::sort(threadCtx->localMapOutput->begin(),
                  threadCtx->localMapOutput->end(), comparePairs());
        if (pthread_mutex_lock(threadCtx->mapOutputMutex))
        {
            std::cerr << "error on pthread_mutex_lock" << std::endl;
            exit(1);
        }
        threadCtx->mapNSortOutput->push_back(*(threadCtx->localMapOutput)); // todo only place where we
        // push to mapNSort, and it's never an empty vector.?????????????????????????????????????????
        if (pthread_mutex_unlock(threadCtx->mapOutputMutex) != 0)
        {
            std::cerr << "error on pthread_mutex_unlock" << std::endl;
            exit(1);
        }
    }
    catch (const std::bad_alloc &e)
    {
        std::cerr << "An error has occurred while sorting." << std::endl;
        exit(1);
    }

    // wait for all the other threads to finish map&sort:
    threadCtx->barrier->barrier();

    // if it's the main thread, start shuffling.
    if (threadCtx->selfId == 0)
    {
        shuffle(threadCtx);
    }

    // reduce phase:
    IntermediateVec sameKeyedpairs;
    while (!(threadCtx->doneShuffling && threadCtx->shuffleOutput->empty()))
    {
        if (!(*threadCtx->doneShuffling))
        {
            if (sem_wait(threadCtx->semaphore))
            {
                std::cerr << "[[Reduce]] error on sem_post" << std::endl;
                exit(1);
            }
//            sem_wait(threadCtx->semaphore);
        }
        if (!(threadCtx->shuffleOutput->empty()))
        {
            if (pthread_mutex_lock(threadCtx->reduceMutex) != 0){
                std::cerr << "[[Reduce]] error on pthread_mutex_lock" << std::endl;
                exit(1);
            }
            sameKeyedpairs = threadCtx->shuffleOutput->back();
            (*threadCtx->shuffleOutput).pop_back();
            if (pthread_mutex_unlock(threadCtx->reduceMutex) != 0) {
                std::cerr << "[[Reduce]] error on pthread_mutex_unlock" << std::endl;
                exit(1);
            }
            // call reduce on it:
            threadCtx->client->reduce(&sameKeyedpairs, threadCtx);
        }
    }
    if (threadCtx->selfId != 0)
    {
        pthread_exit(nullptr);
    }
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
    pthread_mutex_t mapOutputMutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t sem;
    int ret = sem_init(&sem, 0, 0);
    if (ret != 0)
    {
        std::cerr << "An error has occurred while initializing the semaphore." << std::endl;
        sem_destroy(&sem);
        pthread_mutex_destroy(&reduceMutex); // todo: needed?
        pthread_mutex_destroy(&outputVecMutex); // todo: needed?
        exit(1);
    }
    ThreadCtx threadsCtxs[multiThreadLevel]; // all threads contexts
    // initialize context for thread 0:
    vector<IntermediateVec> localMapOutput((unsigned int)multiThreadLevel, IntermediateVec(0));
    threadsCtxs[0]= {&inputVec, &outputVec, &client, &atomic_pairs_counter, 0, multiThreadLevel,
                     &doneShuffling,
                     &mapNsortOutput, &shuffleOutput, &barrier, &reduceMutex, &mapOutputMutex,
                     &outputVecMutex, &sem,
                     &(localMapOutput.at(0))};
    // todo: set values instead of lidros

    // create all threads & initialize their contexts:
    for (int i = 1; i < multiThreadLevel; i++)
    {
        threadsCtxs[i]= {&inputVec, &outputVec, &client, &atomic_pairs_counter, i, multiThreadLevel,
                         &doneShuffling,
                         &mapNsortOutput, &shuffleOutput, &barrier, &reduceMutex, &mapOutputMutex,
                         &outputVecMutex,
                         &sem, &(localMapOutput.at(i))};
        // todo: set values instead of lidros
        ret = pthread_create(&threads[i], nullptr, threadsPart, &threadsCtxs[i]);
        if (ret)
        {
            std::cerr << "An error has occurred while creating a thread." << std::endl;
            exit(1);
        }
    }
    threadsPart(&threadsCtxs[0]);
    for (int i=1; i<multiThreadLevel; i++)
    {
        if (pthread_join(threads[i], nullptr) != 0)
        {
            std::cerr << "error on pthread_join" << std::endl;
            exit(1);
        }
        pthread_join(threads[i], nullptr);
    }
    exitLib(&threadsCtxs[0]);
}
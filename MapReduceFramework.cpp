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

//class Barrier {
//public:
//    Barrier(int numThreads);
//    ~Barrier();
//    void barrier();
//
//private:
//    pthread_mutex_t mutex;
//    pthread_cond_t cv;
//    int count;
//    int numThreads;
//};

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

// -------------------------- struct SharedContext------------------------------
//

//typedef struct SharedContext
//{
//    const InputVec* MapInputVector;
//    OutputVec* outputVec;
//    const MapReduceClient* client;
//    std::atomic<int>* atomicCounter;
//    bool* doneShuffling;
//    std::vector<IntermediateVec>* mapNSortOutput;
//    std::vector<IntermediateVec>* shuffleOutput;
//    Barrier* barrier;
//    pthread_mutex_t* reduceMutex;
//    pthread_mutex_t* outputVecMutex;
//    sem_t* semaphore;
//} SharedContext;

// -------------------------- struct ThreadCtx ------------------------------
//typedef struct ThreadCtx
//{
//    const InputVec* MapInputVector;
//    OutputVec* outputVec;
//    const MapReduceClient* client;
//    std::atomic<int>* atomicCounter;
//    int mainThreadId;
//    int selfId;
//    bool* doneShuffling;
//    std::vector<IntermediateVec>* mapNSortOutput;
//    std::vector<IntermediateVec>* shuffleOutput;
//    Barrier* barrier;
//    pthread_mutex_t* reduceMutex;
//    pthread_mutex_t* outputVecMutex;
//    sem_t* semaphore;
//    IntermediateVec* localMapOutput;
//}ThreadCtx;


// -------------------------- inner funcs ------------------------------
// declarations so we can keep up with our funcs

void* threadsPart(void* arg);
bool areKeysEqual(K2* key1, K2* key2);
void shuffle(ThreadCtx* genCtx);
void exitLib(ThreadCtx* threadCtx, int exitCode);

// ---------------------------- helper methods --------------------------

typedef struct comparePairs {
    bool operator()(const std::pair<K2*,V2*>& lhs, const std::pair<K2*,V2*>&
    rhs) const
    {
        return lhs.first < rhs.first;
    }
} comparePairs;

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

    while (!(genCtx->mapNSortOutput->empty()))
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
        cerr << "Thread 1: locked reduceMutex" << endl;
        genCtx->shuffleOutput->push_back(currentVector);
        cerr << "Thread 1: pushed shuffle output " << endl;
        pthread_mutex_unlock(genCtx->reduceMutex);
        cerr << "Thread 1: released  reduceMutex" << endl;
        sem_post(genCtx->semaphore);
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
// semaphore not always freed

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
//    int old_value = 0;
    // call map on MapInputVector's elements:
    K1* key;
    V1* val;
    const InputVec input = (*threadCtx->MapInputVector);
//    InputPair* pair;
//    while ((threadCtx->atomicCounter->load()) <
//            (int) threadCtx->MapInputVector->size())
//    int bound = (int) threadCtx->MapInputVector->size();
    int old_value = (*(threadCtx->atomicCounter))++;
    while ((unsigned int) old_value < input.size())
    {
//        cerr << threadCtx->selfId << ": old value: " << old_value << endl;
//        old_value = (*(threadCtx->atomicCounter))++;
        threadCtx->client->map(input.at(old_value).first, input.at(old_value)
                .second, threadCtx);
//        if (old_value < ((int) threadCtx->MapInputVector->size()))
//        {
        old_value = (*(threadCtx->atomicCounter))++;
//        key = threadCtx->MapInputVector->at(old_value).first;
//        val = threadCtx->MapInputVector->at(old_value).second;
//        }
//        else
//        {
//            std::cerr << "break" << endl;
//            break;
//        }

        // sort the resulting vector of pairs and push it to the queue of intermediate vectors:
        try
        {
//            std::sort(threadCtx->localMapOutput->begin(),
//                      threadCtx->localMapOutput->end(), comparePairs);
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
    cerr << "post barrier thread id: " << threadCtx->selfId << endl;

    // if it's the main thread, start shuffling.
//    cerr << "thread id: " << threadCtx->selfId << endl;
    if (threadCtx->selfId == 0)
    {
//        cerr << threadCtx->selfId << ": in shuffle " << endl;
        shuffle(threadCtx);
        *(threadCtx->doneShuffling) = true;
    }

    // reduce:
    IntermediateVec sameKeyedpairs;
    while (!(threadCtx->doneShuffling &&
             threadCtx->mapNSortOutput->empty()))
    {
        // pop the last vector of same keyed pairs from the intermediate-vectors vector:
        cerr << "waiting on semaphore: " << threadCtx->selfId << endl;
        sem_wait(threadCtx->semaphore);
        if (!(threadCtx->shuffleOutput->empty()))
        {
            pthread_mutex_lock(threadCtx->reduceMutex);
            cerr << threadCtx->selfId<< ": locked reduceMutex" << endl;
            cerr << threadCtx->selfId << ": shuffle output size: " <<
                                                threadCtx->shuffleOutput->size
                    () << endl;
            sameKeyedpairs = threadCtx->shuffleOutput->back();
//            cerr << threadCtx->selfId<< ": sameKeyedpairs: " << sameKeyedpairs
//                    .front().first << endl;
            (*threadCtx->shuffleOutput).pop_back();
            cerr << threadCtx->selfId<< ": shuffle output size: " <<
                 threadCtx->shuffleOutput->size
                         () << endl;
            pthread_mutex_unlock(threadCtx->reduceMutex);
            cerr << threadCtx->selfId << ": released reduceMutex" << endl;
            // call reduce on it:
            threadCtx->client->reduce(&sameKeyedpairs, threadCtx);
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
//    pthread_t mainThreadId = threadCtx->selfId;
    bool doneShuffling = false;
    std::vector<IntermediateVec> mapNsortOutput = {};
    std::vector<IntermediateVec> shuffleOutput = {};

    // initialize barrier, mutexes & semaphore:
//    auto barrier = new Barrier(multiThreadLevel);
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

    ThreadCtx threadsCtxs[multiThreadLevel];
    vector<IntermediateVec> localMapOutput(multiThreadLevel, IntermediateVec
            (0));
//    for (int i=0; i<multiThreadLevel; i++){
//        localMapOutput.push_back({});
//        cerr << "l: " << localMapOutput.at(i) << endl;
//    }

    threadsCtxs[0]= {&inputVec, &outputVec, &client,
                     &atomic_pairs_counter, 0,
                     &doneShuffling, &mapNsortOutput, &shuffleOutput,
                     &barrier, &reduceMutex,
                     &outputVecMutex, &sem, &(localMapOutput.at(0))};
    // todo: set values instead of lidros
//    ret = pthread_create(&threads[0], nullptr, threadsPart,
//                         &threadsCtxs[0]);

    // create threads:
    for (int i = 1; i < multiThreadLevel; i++)
    {
//        cerr << "i: " << i << endl;
        threadsCtxs[i]= {&inputVec, &outputVec, &client,
                         &atomic_pairs_counter, i,
                         &doneShuffling, &mapNsortOutput, &shuffleOutput,
                         &barrier, &reduceMutex,
                         &outputVecMutex, &sem, &(localMapOutput.at(i))};
        // todo: set values instead of lidros
        cerr << threadsCtxs[i].selfId << endl;
        ret = pthread_create(&threads[i], nullptr, threadsPart,
                             &threadsCtxs[i]);
        if (ret)
        {
            fprintf(stderr, "An error has occurred while creating a thread.");
            exitLib(&threadsCtxs[i], 1);
        }
    }
    threadsPart(&threadsCtxs[0]);
    exitLib(&threadsCtxs[0], 0);
}
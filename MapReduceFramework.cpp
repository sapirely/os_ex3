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

// -------------------------- class GeneralContext------------------------------
class GeneralContext
{
public:
    const InputVec* inputVec;
    OutputVec* outputVec;
    const MapReduceClient* client;
    std::atomic<int>* atomic_counter;
    pthread_t mainThreadId;
    bool* doneShuffling;
    std::vector<IntermediateVec>* intermediateVecQueue;
    Barrier* barrier;
    pthread_mutex_t* vecQueueMutex;
    pthread_mutex_t* intermediateVecMutex;
    pthread_mutex_t* outputVecMutex;
    sem_t* semaphore;
    GeneralContext(const InputVec* inputVec, OutputVec* outputVec,
                   const MapReduceClient* client, std::atomic<int>* atomic_counter,
                   pthread_t mainThreadId, bool* doneShuffling,
                   std::vector<IntermediateVec>* intermediateVecQueue,
                   Barrier* barrier, pthread_mutex_t* vecQueueMutex,
                   pthread_mutex_t* IntermediateVecMutex,
                   pthread_mutex_t* outputVecMutex, sem_t* semaphore):
            inputVec(inputVec), outputVec(outputVec), client(client),
            atomic_counter(atomic_counter), mainThreadId(mainThreadId),
            doneShuffling(doneShuffling), intermediateVecQueue
                    (intermediateVecQueue), barrier(barrier), vecQueueMutex
                    (vecQueueMutex), intermediateVecMutex
                    (IntermediateVecMutex), outputVecMutex(outputVecMutex),
            semaphore(semaphore){}
};

// -------------------------- class ThreadCtx ------------------------------
class ThreadCtx
{
public:
    GeneralContext* generalCtx;
    IntermediateVec* intermediatePairs;
    ThreadCtx(GeneralContext* generalCtx,
    IntermediateVec* intermediatePairs): generalCtx(generalCtx),
                                        intermediatePairs(intermediatePairs){}
    ThreadCtx()
    {
        generalCtx = nullptr;
        intermediatePairs = nullptr;
    }
};



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

// ---------------------------- helper methods --------------------------


// primary version
// updates vector of vectors of pairs: key and value
//void shuffle(IntermediateVec* inputVec, ThreadCtx* context){
//
//    // gets sorted intermediary vectors
//    // elements are popped from the back of each vector
//    //todo:
//    // use semaphore for counting vectors
//    // whenever a new vector is inserted to the queue, call sem_post()
//    // use mutex to protect access to the queue
//
//    K2* currentKey = nullptr;
//
//    // init first vector for first key
////    sem_wait(context->generalCtx->semaphore);
////    context->generalCtx->intermediateVecQueue->emplace_back();
////    sem_post(context->generalCtx->semaphore);
//
//    IntermediateVec currentVector= {};
//    // elements are popped from the back of each vector
//    while (!inputVec->empty())
//    {
//
//        // iterate over intermediate vec - insert all values with a certain key
//        // in a sequence
//        if (currentKey == nullptr) // first key
//        {
//            currentKey = inputVec->back().first;
//        }
//        else if (currentKey != inputVec->back().first) // new key
//        {
//            // prev key is done -> should change keys
//            currentKey = inputVec->back().first;
//            sem_wait(context->generalCtx->semaphore);
//            context->generalCtx->intermediateVecQueue->push_back(currentVector);
//            sem_post(context->generalCtx->semaphore);
//            currentVector = {};
//        }
//        // now key is identical to the one at the back of the vector
//
//        // enter value to the sequence (the vector of key CurrentKey)
//        sem_wait(context->generalCtx->semaphore);
//        context->generalCtx->intermediateVecQueue->back().push_back
//                ((inputVec->back());
//        currentVector.push_back(inputVec->back());
//        sem_post(context->generalCtx->semaphore);
//        inputVec->pop_back();
//
//    }
//    // todo - from instructions:
//    // run parallel with Reduce
//}


void shuffle(GeneralContext* genCtx){
    std::vector<IntermediateVec> shuffledVectors;
    IntermediateVec currentVector;
    K2* maxKey = genCtx->intermediateVecQueue->at(0).back().first;

    K2* nextKey = maxKey;
    // find min key
    for (auto vector : *(genCtx->intermediateVecQueue))
    {
        if (maxKey < vector.back().first)
        {
            maxKey = vector.back().first;
        }
    }

    int numOfVectors = (int) genCtx->intermediateVecQueue->size();

    while (!genCtx->intermediateVecQueue->empty())
    {
        currentVector = {};
        // add all pairs with maxKey to currentVector
        for (int j = 0; j < numOfVectors; j++)
        {
            // pop all vectors with key maxKey into currentVector
            sem_wait(genCtx->semaphore);
            while (maxKey == genCtx->intermediateVecQueue->at(j).back().first)
            {
                currentVector.emplace_back(
                        genCtx->intermediateVecQueue->at(j).back());
                genCtx->intermediateVecQueue->at(j).pop_back();
            }
            // if the next key in the current vector is bigger than maxKey,
            // put it in nextKey
            if (nextKey < genCtx->intermediateVecQueue->at(j).back().first)
            {
                nextKey = genCtx->intermediateVecQueue->at(j).back().first;
            }
            sem_post(genCtx->semaphore);
        }
        shuffledVectors.push_back(currentVector);
    }
}

void reduce(GeneralContext* context){
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
void exitLib(ThreadCtx* threadCtx, int exitCode)
{
    delete threadCtx->generalCtx->barrier;
    sem_destroy(threadCtx->generalCtx->semaphore);
    pthread_mutex_destroy(threadCtx->generalCtx->intermediateVecMutex);
    pthread_mutex_destroy(threadCtx->generalCtx->vecQueueMutex);
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
    tc->intermediatePairs->emplace_back(make_pair(key,value)); //todo

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

    // call map:
    K1* key;
    V1* val;
    IntermediateVec localIntermediateVec = {};
    while (*threadCtx->generalCtx->atomic_counter <=
            threadCtx->generalCtx->inputVec->size())
    {
        old_value = (*(threadCtx->generalCtx->atomic_counter))++;
        key = (*threadCtx->generalCtx->inputVec)[old_value].first;
        val = (*threadCtx->generalCtx->inputVec)[old_value].second;
        (*threadCtx->generalCtx->client).map(key, val, &threadCtx);
        // map should get context
    }

    // sort:
    try {
        std::sort((localIntermediateVec).begin(), (localIntermediateVec).end());
        // sort pairs - does it sort using the k2 operator?
    }
    catch (const std::bad_alloc& e) {
        std::cerr << "An error has occurred while sorting." << std::endl;
        exitLib(threadCtx, 1);
    }

    if (pthread_self() == threadCtx->generalCtx->mainThreadId) // if it's the
        // main thread, start shuffling.
    {
        shuffle(&localIntermediateVec, threadCtx);
        *(threadCtx->generalCtx->doneShuffling) = true;
    }
    threadCtx->generalCtx->barrier->barrier();

    // reduce:
    while (!(threadCtx->generalCtx->doneShuffling &&
            (*threadCtx->generalCtx->intermediateVecQueue).empty()))
    {
        sem_wait(threadCtx->generalCtx->semaphore);
        pthread_mutex_lock(threadCtx->generalCtx->intermediateVecMutex);
        IntermediateVec* pair = &((*threadCtx->generalCtx
                ->intermediateVecQueue).back()); // pair is a key (char)
        // and a vector intermediateVec - counts from all threads of the key char.
        (*threadCtx->generalCtx->intermediateVecQueue).pop_back();
        pthread_mutex_unlock(threadCtx->generalCtx->intermediateVecMutex);
        threadCtx->generalCtx->client->reduce(pair, &threadCtx);
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
    std::vector<IntermediateVec>* intermediateVecQueue = {};
    std::vector<IntermediateVec> intermediateVec = {};

    // initialize barrier, mutexes & semaphore:
    auto barrier = new Barrier(multiThreadLevel);
    pthread_mutex_t vecQueueMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t intermediateVecMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t outputVecMutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t sem;
    int ret = sem_init(&sem, 0, 1);
    if (ret != 0)
    {
        fprintf(stderr, "An error has occurred while initializing the semaphore.");
        sem_destroy(&sem);
        pthread_mutex_destroy(&intermediateVecMutex);
        pthread_mutex_destroy(&vecQueueMutex);
        pthread_mutex_destroy(&outputVecMutex);
        exit(1);
//        exitLib(&threadsCtxs[0], 1);
    }

    // create context for all threads:
    GeneralContext* generalContext = new GeneralContext(&inputVec,
                                                        &outputVec, &client,
                                                       &atomic_pairs_counter,
                                                       mainThreadId, &doneShuffling,
                                                       intermediateVecQueue, barrier, &vecQueueMutex,
                                                       &intermediateVecMutex,
                                                       &outputVecMutex, &sem);

//    GeneralContext generalContext = {&inputVec, &outputVec, &client,
//                                &atomic_pairs_counter,
//                  mainThreadId, &doneShuffling,
//                  intermediateVecQueue, barrier, &vecQueueMutex,
//                  &intermediateVecMutex, &outputVecMutex, &sem};
    ThreadCtx threadsCtxs[multiThreadLevel];

    // create threads:
    for (int i = 0; i < multiThreadLevel - 1; i++)
    {
        *(threadsCtxs[0].intermediatePairs) = IntermediateVec();
        threadsCtxs[0].generalCtx = generalContext;
        ret = pthread_create(&threads[i], nullptr, threadsPart,
                             &threadsCtxs[i]);
        if (!ret)
        {
            fprintf(stderr, "An error has occurred while creating a thread.");
            exitLib(&threadsCtxs[i], 1);
        }
    }
    threadsPart(&threadsCtxs[0]);

    exitLib(&threadsCtxs[0], 0);

}

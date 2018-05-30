/**
 * @file MapReduceFramework.cpp
 *
 */

// ------------------------------ includes ----------------------------

#include <iostream>
#include <vector>
#include "MapReduceFramework.h"

using namespace std;
// ------------------------------- globals -----------------------------

// -------------------------- inner funcs ------------------------------

// declarations so we can keep up with our funcs


// ---------------------------- helper methods --------------------------

// primary version
void shuffle(IntermediateVec intermediateVec){

    // gets sorted intermediary vectors
    // elements are popped from the back of each vector
    //todo:
    // use semaphore for counting vectors
    // whenever a new vector is inserted to the queue, call sem_post()
    // use mutex to protect access to the queue

    K2* currentKey = nullptr;
    // vector of pairs: key, and a vector of values
    std::vector<std::pair<K2*, std::vector<V2*>>> vecQueue;
    // maybe shouldn't be declared here - how will it work with the threads?
    // todo

    // elements are popped from the back of each vector
    while (!intermediateVec.empty())
    {
        // iterate over intermediate vec - insert all values with a certain key
        // in a sequence
        if (currentKey == nullptr) // first key
        {
            currentKey = intermediateVec.back().first;
            // create new sequence
            pair<K2*, std::vector<V2*>> newSequence;
            vecQueue.push_back(newSequence);
        }
        else if (currentKey != intermediateVec.back().first)
        {
                // prev key is done -> should change keys
                currentKey = intermediateVec.back().first;
                pair<K2*, std::vector<V2*>> newSequence;
                vecQueue.push_back(newSequence);
        }
        // now key is identical to the one at the back of the vector

        // enter value to the sequence (the vector of key CurrentKey)
        vecQueue.back().second.push_back(intermediateVec.back().second);
        intermediateVec.pop_back();

    }


    // todo - from instructions:
    // difficult to split efficiently into parallel threads - so we
    // run parallel with Reduce
}

// ---------------------------- library methods -------------------------



/**
 * Called in Map phase
 */
void emit2 (K2* key, V2* value, void* context){
    return;
}

/**
 * Called in Reduce phase
 */
void emit3 (K3* key, V3* value, void* context){
    return;
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


    return;
}
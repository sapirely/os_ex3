/**
 * @file MapReduceFramework.cpp
 *
 */

// ------------------------------ includes ----------------------------

#include <iostream>
#include <vector>
#include "MapReduceFramework.h"


// ------------------------------- globals -----------------------------

// -------------------------- inner funcs ------------------------------

// declarations so we can keep up with our funcs


// ---------------------------- helper methods --------------------------

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
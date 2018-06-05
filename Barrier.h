//
// Created by sapirely on 6/5/18.
//

#ifndef OS_EX3_BARRIER_H
#define OS_EX3_BARRIER_H

#include <pthread.h>

class Barrier
{
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


#endif //OS_EX3_BARRIER_H

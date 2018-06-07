#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <iostream>


class VString : public V1 {
public:
	VString(std::string content) : content(content) { }
	std::string content;
};

class KChar : public K2, public K3{
public:
	KChar(char c) : c(c) { }
	virtual bool operator<(const K2 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}

    

	virtual bool operator<(const K3 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	char c;
};

class VCount : public V2, public V3{
public:
	VCount(int count) : count(count) { }
	int count;
};


class CounterClient : public MapReduceClient {
public:
	void map(const K1* key, const V1* value, void* context) const {
		std::array<int, 256> counts;
		counts.fill(0);
		for(const char& c : static_cast<const VString*>(value)->content) {
			counts[(unsigned char) c]++;
		}

		for (int i = 0; i < 256; ++i) {
			if (counts[i] == 0)
				continue;

			KChar* k2 = new KChar(i);
			VCount* v2 = new VCount(counts[i]);
			emit2(k2, v2, context);
		}
	}

	virtual void reduce(const IntermediateVec* pairs,
		void* context) const {
		const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
		int count = 0;
		for(const IntermediatePair& pair: *pairs) {
			count += static_cast<const VCount*>(pair.second)->count;
			delete pair.first;
			delete pair.second;
		}
		KChar* k3 = new KChar(c);
		VCount* v3 = new VCount(count);
		emit3(k3, v3, context);
	}

    // todo remove:
//    virtual void print(K2* key){
//        KChar* k = (KChar*) key;
//        std::cout << "key: " << k->c << std::endl;
//    }

//    const void print(K3* key){
//        KChar* k = (KChar*) key;
//        std::cout << "key: " << k->c << std::endl;
//    }
//
//    const void print(V1* val){
//        VString* v = (VString*) val;
//        std::cout << "key: " << v->content << std::endl;
//    }
//
//    const void print(V2* val){
//        VCount* v = (VCount*) val;
//        std::cout << "key: " << v->count << std::endl;
//    }
//
//    const void print(V3* val){
//        VCount* v = (VCount*) val;
//        std::cout << "key: " << v->count << std::endl;
//    }

};




int main(int argc, char** argv)
{
    for (int i=0; i<200; i++)
    {
        CounterClient client;
        InputVec inputVec;
        OutputVec outputVec;
        VString s1("This string is full of characters");
        VString s2("Multithreading is awesome");
        VString s3("conditions are race bad");
        inputVec.push_back({nullptr, &s1});
        inputVec.push_back({nullptr, &s2});
        inputVec.push_back({nullptr, &s3});
        runMapReduceFramework(client, inputVec, outputVec, 4);
        if (outputVec.size() != 21) {

            printf("ERROR size of output:%d\n", outputVec.size());
        }

        for (OutputPair &pair: outputVec)
        {
            char c = ((const KChar *) pair.first)->c;
            int count = ((const VCount *) pair.second)->count;
            //		printf("The character %c appeared %d time%s\n",
            //			c, count, count > 1 ? "s" : "");
            delete pair.first;
            delete pair.second;
        }
        inputVec.clear();
        outputVec.clear();
    }
	return 0;
}


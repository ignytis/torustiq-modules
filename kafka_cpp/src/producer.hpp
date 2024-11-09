#ifndef _TORUSTIQ_KAFKA_CPP_PRODUCER_H_
#define _TORUSTIQ_KAFKA_CPP_PRODUCER_H_

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <thread>

#include <librdkafka/rdkafkacpp.h>

#include "utils/strings.hpp"
#include "../../torustiq_common_typedefs.hpp"

using namespace std;

namespace torustiq_kafka_cpp
{
class Producer {
    public:
        Producer(map<string, string>);

        void start();
        void produce(torustiq_common::ByteBuffer *buffer);
    private:
        map<string, string> driver_params;

        RdKafka::Producer *rd_producer = nullptr;
};

void kafka_thread(RdKafka::Producer *producer);
}

#endif
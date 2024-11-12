#ifndef _TORUSTIQ_KAFKA_CPP_PRODUCER_H_
#define _TORUSTIQ_KAFKA_CPP_PRODUCER_H_

#include <algorithm>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
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

        optional<string> start();
        // Returns an error string if error occurred
        optional<string> produce(const string topic, const optional<string> *key, const map<string, string> *headers,
            const torustiq_common::ByteBuffer *buffer);
        void rd_producer_lock();
        void rd_producer_unlock();
        void rd_producer_poll();

        mutex rd_producer_mtx;
    private:
        map<string, string> driver_params;

        RdKafka::Producer *rd_producer = nullptr;
        thread rd_producer_poll_thread;
};

void kafka_poll_thread(Producer *producer);
}

#endif
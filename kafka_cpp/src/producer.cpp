#include "producer.hpp"

// using namespace torustiq_kafka_cpp;

namespace torustiq_kafka_cpp
{

Producer::Producer(map<string, string> driver_params)
{
    this->driver_params = driver_params;
}

// https://gist.github.com/GenesisFR/cceaf433d5b42dcdddecdddee0657292
string replace_all(string str, const string &from, const string &to)
{
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != string::npos)
    {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}

void Producer::start()
{
    string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    map<string, string>::iterator it;
    for(it = this->driver_params.begin(); it != this->driver_params.end(); it++)
    {
        string k = it->first;
        k = replace_all(k, "_", ".");
        string v = it->second;

        cout << "Setting '" << k << "' to " << v << endl ; 
        if (conf->set(k.c_str(), v.c_str(), errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
    }

    this->rd_producer = RdKafka::Producer::create(conf, errstr);
    if (!this->rd_producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;
}

void Producer::produce(torustiq_common::ByteBuffer *buffer)
{
    string topic("my_topic_one");
    RdKafka::ErrorCode err = this->rd_producer->produce(
        /* Topic name */
        topic,
        /* Any Partition: the builtin partitioner will be
         * used to assign the message to a topic based
         * on the message key, or random partition if
         * the key is not set. */
        RdKafka::Topic::PARTITION_UA,
        /* Make a copy of the value */
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        /* Value */
        buffer->bytes, buffer->len,
        /* Key */
        NULL, 0,
        /* Timestamp (defaults to current time) */
        0,
        /* Message headers, if any */
        NULL,
        /* Per-message opaque value passed to
         * delivery report */
        NULL);

    if (err != RdKafka::ERR_NO_ERROR) {
      std::cerr << "% Failed to produce to topic " << topic << ": "
                << RdKafka::err2str(err) << std::endl;

      if (err == RdKafka::ERR__QUEUE_FULL) {
        /* If the internal queue is full, wait for
         * messages to be delivered and then retry.
         * The internal queue represents both
         * messages to be sent and messages that have
         * been sent or failed, awaiting their
         * delivery report callback to be called.
         *
         * The internal queue is limited by the
         * configuration property
         * queue.buffering.max.messages and queue.buffering.max.kbytes */
        this->rd_producer->poll(1000 /*block for max 1000ms*/);
      }

    } else {
      std::cerr << "% Enqueued message (" << buffer->len << " bytes) "
                << "for topic " << topic << std::endl;
    }

    /* A producer application should continually serve
     * the delivery report queue by calling poll()
     * at frequent intervals.
     * Either put the poll call in your main loop, or in a
     * dedicated thread, or call it after every produce() call.
     * Just make sure that poll() is still called
     * during periods where you are not producing any messages
     * to make sure previously produced messages have their
     * delivery report callback served (and any other callbacks
     * you register). */
    this->rd_producer->poll(0);
}
}
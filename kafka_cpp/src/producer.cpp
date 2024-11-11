#include "producer.hpp"

// using namespace torustiq_kafka_cpp;

namespace torustiq_kafka_cpp
{

Producer::Producer(map<string, string> driver_params)
{
    this->driver_params = driver_params;
}

optional<string> Producer::start()
{
    string errstr;

    // Set Kafka properties from driver parameter section
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    map<string, string>::iterator it;
    for(it = this->driver_params.begin(); it != this->driver_params.end(); it++)
    {
        string k = torustiq_kafka_cpp::utils::strings::replace_all(it->first, "_", ".");
        string v = it->second;
        if (conf->set(k.c_str(), v.c_str(), errstr) != RdKafka::Conf::CONF_OK) {
            return string(errstr);
        }
    }

    this->rd_producer = RdKafka::Producer::create(conf, errstr);
    if (!this->rd_producer) {
        return string(errstr);
    }

    delete conf;
    return nullopt;
}

void Producer::produce(const string topic, const optional<string> *key, const map<string, string> *headers, const torustiq_common::ByteBuffer *buffer)
{
    RdKafka::Headers *rd_headers = RdKafka::Headers::create();
    for (pair<const string, string> item: *headers)
    {
        rd_headers->add(item.first, item.second);
    }

    RdKafka::ErrorCode err = this->rd_producer->produce(topic, RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY, buffer->bytes, buffer->len, key, 0,
        0, rd_headers, NULL);

    if (err != RdKafka::ERR_NO_ERROR) {
      std::cerr << "% Failed to produce to topic " << topic << ": "
                << RdKafka::err2str(err) << std::endl;
    }

    // TODO: move to thread
    this->rd_producer->poll(0);
}
}
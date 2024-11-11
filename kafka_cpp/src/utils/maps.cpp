#include "maps.hpp"

namespace torustiq_kafka_cpp::utils::maps
{

template<typename T, typename U>
bool key_exists(T k, map<T, U> m)
{
    return m.find(k) != m.end();
}

bool key_exists(const char *k, map<string, string> m)
{
    return m.find(k) != m.end();
}

}
#ifndef _TORUSTIQ_KAFKA_CPP_UTILS_MAPS_H_
#define _TORUSTIQ_KAFKA_CPP_UTILS_MAPS_H_

#include <map>
#include <string>

using namespace std;

namespace torustiq_kafka_cpp::utils::maps
{
template<typename T, typename U>
bool key_exists(T k, map<T, U> m);
bool key_exists(const char *k, map<string, string> m);
}

#endif
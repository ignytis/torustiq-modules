#ifndef _TORUSTIQ_KAFKA_CPP_UTILS_STRING_H_
#define _TORUSTIQ_KAFKA_CPP_UTILS_STRING_H_

#include <string>

using namespace std;

namespace torustiq_kafka_cpp::utils::strings
{
string replace_all(string str, const string &from, const string &to);
}

#endif
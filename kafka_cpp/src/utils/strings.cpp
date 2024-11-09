#include "strings.hpp"

namespace torustiq_kafka_cpp::utils::strings
{

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

}
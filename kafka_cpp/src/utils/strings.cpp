#include "strings.hpp"

namespace torustiq_kafka_cpp::utils::strings
{

bool begins_with(string str, string substr)
{
    return 0 == str.rfind(substr, 0);
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

string strip_prefix(string str, string prefix)
{
    if (!begins_with(str, prefix))
    {
        return str;
    }

    return str.substr(prefix.length());
}

}
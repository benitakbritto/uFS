#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <chrono>

std::string generateString(const std::string str1, int repeatCount);
int initClient(const std::string &pids);
void printElapsedTime(std::string functionName, int64_t elapsedTime);

#endif
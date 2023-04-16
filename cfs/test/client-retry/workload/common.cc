#include "common.h"
#include <assert.h>
#include "util.h"
#include "../../client/testClient_common.h"
#include <fsapi.h>
#include <sstream>


std::string generateString(const std::string str1, int repeatCount) {
  std::ostringstream buffer;
  for (int i = 0; i < repeatCount; i++) {
    buffer << str1;
  }
  return buffer.str();
}

int initClient(const std::string &pids) {
  const int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  for (int i = 0; i < kMaxNumFsp; i++) shmKeys[i] = 0;
  auto pidStrVec = splitStr(pids, ',');
  int numKey = pidStrVec.size();
  assert(numKey <= kMaxNumFsp);

  for (int i = 0; i < numKey; i++) {
    shmKeys[i] = (FS_SHM_KEY_BASE) + stoiWrapper(pidStrVec[i]);
  }

  int rt = fs_init_multi(numKey, shmKeys);
  if (rt < 0) {
    fprintf(stderr, "fs_init_multi failure\n");
    return -1; // failure
  } else {
    fprintf(stdout, "fs_init_multi success\n");
  }

  fprintf(stdout, "[INFO] client connection successful\n");
  return 0; // success
}

void printElapsedTime(std::string functionName, int64_t elapsedTime) {
  std::cout << functionName << " : " << elapsedTime << " ns" << std::endl; 
}
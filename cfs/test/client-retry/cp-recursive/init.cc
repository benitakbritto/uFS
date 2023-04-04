// Create the FSPsrc dir here
#include <assert.h>
#include <fcntl.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>

#include "fsapi.h"
#include "../../client/testClient_common.h"
#include "util.h"

// Function prototypes
int initClient(const std::string &pids);
int callMkdir(const char *path);
std::string generateString(std::string &str1, int repeatCount, std::string &str2);
int writeToFile(const char *path, const char *buf, ssize_t size);
int initFileStructure();

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

int callMkdir(const char *path) {
  return fs_mkdir(path, 0755);
}

std::string generateString(const std::string str1, int repeatCount, 
  const std::string str2) {
  std::ostringstream buffer;
  for (int i = 0; i < repeatCount; i++) {
    buffer << str1;
  }
  return buffer.str() + str2;
}

int writeToFile(const char *path, const char *buf, ssize_t size) {
  // Assumes that file does not exist
  auto ino = fs_open(path, O_CREAT, 0644);
  if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }

  if (fs_write(ino, buf, size) != size) {
    fprintf(stderr, "fs_write() failed\n");
    return -1; // failure
  }

  return 0;
}

int initFileStructure() {
  if (callMkdir("FSPsrc") != 0) {
    fprintf(stderr, "Failed FSPsrc\n");
    return -1;
  }

  if (callMkdir("FSPsrc/FSPdirl1_0") != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_0\n");
    return -1;
  }

  if (callMkdir("FSPsrc/FSPdirl1_1") != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_1\n");
    return -1;
  }

  if (callMkdir("FSPsrc/FSPdirl1_1/FSPdirl2_0") != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_1/FSPdirl2_0\n");
    return -1;
  }

  if (callMkdir("FSPsrc/FSPdirl1_1/FSPdirl2_1") != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_1/FSPdirl2_1\n");
    return -1;
  }

  auto str0 = generateString("ab", 100000, "z");
  if (writeToFile("FSPsrc/FSPdirl1_0/FSPf0", str0.c_str(), 
    str0.length() + 1) != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_0/FSPf0\n");
    return -1;
  }

  auto str1 = generateString("cd", 200000, "y");
  if (writeToFile("FSPsrc/FSPdirl1_0/FSPf1", str1.c_str(), 
    str1.length() + 1) != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_0/FSPf1\n");
    return -1;
  }

  auto str2 = generateString("ef", 110000, "x");
  if (writeToFile("FSPsrc/FSPdirl1_1/FSPf2", str2.c_str(), 
    str2.length() + 1) != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_1/FSPf2\n");
    return -1;
  }

  auto str3 = generateString("gh", 210000, "y");
  if (writeToFile("FSPsrc/FSPdirl1_1/FSPdirl2_0/FSPf3", str3.c_str(), 
    str3.length() + 1) != 0) {
    fprintf(stderr, "Failed FSPsrc/FSPdirl1_1/FSPdirl2_0/FSPf3\n");
    return -1;
  }

  fs_syncall();

  fprintf(stdout, "[INFO] Created file strucure successfully\n");

  return 0;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage %s <pid1,pid2,..>\n", argv[0]);
    fprintf(stderr, "\t requires only one argument\n");
    fprintf(stderr,
            "\t Option-2 --- pid1,pid2,..: a list of integers separated by , "
            "(comma). The last pid will be used for the pending queue.\n");
    exit(1);
  }

  if (initClient(argv[1]) != 0) {
    exit(1);
  }

  if (initFileStructure() != 0) {
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
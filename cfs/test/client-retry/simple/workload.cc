#include <assert.h>
#include <chrono>
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
int callOpen(const char *path);
int callCreate(const char *path);
int callClose(const char *path);
int callAllocatedRead(int fd, int count);
int callAllocatedWrite(int fd, void *buf, int count);
int callStat(const char *path, struct stat *statbuf);
int callFstat(int fd, struct stat *statbuf);
std::string generateString(std::string &str1, int repeatCount, std::string &str2);
int initFileStructure();
void printElapsedTime(std::string functionName, int64_t elapsedTime);

void printElapsedTime(std::string functionName, int64_t elapsedTime) {
  std::cout << functionName << " : " << elapsedTime << std::endl; 
}

std::string generateString(const std::string str1, int repeatCount, 
  const std::string str2) {
  std::ostringstream buffer;
  for (int i = 0; i < repeatCount; i++) {
    buffer << str1;
  }
  return buffer.str() + str2;
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

int callMkdir(const char *path) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  auto ret = fs_mkdir(path, 0755);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("mkdir", elapsedTime);
  
  return ret;
}

int callOpen(const char *path) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  auto ret = fs_open(path, O_RDWR, 0);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("open", elapsedTime);
  
  return ret;
}

int callCreate(const char *path) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  auto ret = fs_open(path, O_CREAT, 0);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("create", elapsedTime);
  
  return ret;
}

int callAllocatedRead(int fd, int count) {
  char *buf = (char *)fs_malloc(count + 1);
  assert(buf != nullptr);
  memset(buf, 0, count + 1);

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  auto ret = fs_allocated_read(fd, buf, count, (void **) &buf);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("allocread", elapsedTime);

  fs_free(buf);
  return ret;
}

int callAllocatedWrite(int fd, void *buf, int count) {
  void *cur_buf = fs_malloc(count + 1); 
	assert(cur_buf != nullptr); 
	memcpy(cur_buf, (void*)buf, count); 

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	auto ret = fs_allocated_write(fd, cur_buf, count); 
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("allocwrite", elapsedTime);

  return ret;
}

int callStat(const char *path, struct stat *statbuf) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	auto ret = fs_stat(path, statbuf); 
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("stat", elapsedTime);

  return ret;
}

int callFstat(int fd, struct stat *statbuf) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	auto ret = fs_fstat(fd, statbuf); 
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("fstat", elapsedTime);

  return ret;
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

  // TODO: run workload

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
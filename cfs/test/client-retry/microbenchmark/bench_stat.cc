// Create the FSPsrc dir here
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

// Macros
#define ITERATIONS 50

// Function prototypes
int runWorkload(ssize_t iter);

// Main
int runWorkload(ssize_t iter) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  
  std::string parentPath = "";
  std::string currentPath = "";
  for (int i = 0; i < iter; i++) {
    if (i % 10 == 0) {
      parentPath += "dir" + std::to_string(i / 10) + "/";
    }

    currentPath = parentPath + "f" + std::to_string(i % 10);

    struct stat statbuf;
    auto ret = fs_stat(currentPath.c_str(), &statbuf);
    if (ret != 0) {
        fprintf(stderr, "fs_stat() failed\n");
        return -1; // failure
    }
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("stat", elapsedTime);
  
  return 0; // success
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

  if (runWorkload(ITERATIONS) != 0) {
    fprintf(stderr, "runWorkload() failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
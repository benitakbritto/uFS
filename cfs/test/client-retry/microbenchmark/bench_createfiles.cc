/*
* Creates 50 files, 10 files per dir
* Writes 1MB at once to each file
*/
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

// Macros
#define IO_SIZE (1024 * 1024)
#define ITERATIONS 50

// Function prototypes
int runWorkload(ssize_t ioSize, ssize_t iter);

// Main
int runWorkload(ssize_t ioSize, ssize_t iter) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::string parentPath = "";
  std::string currentPath = "";
  for (int i = 0; i < iter; i++) {
    if (i % 10 == 0) {
      parentPath += "d" + std::to_string(i / 10) + "/";
      if (fs_mkdir(parentPath.c_str(), 0755) < 0) {
        fprintf(stderr, "fs_mkdir() failed\n");
        return -1; // failure
      }
    }

    currentPath = parentPath + "f" + std::to_string(i % 10);

    auto ino = fs_open(currentPath.c_str(), O_CREAT, 0);
    if (ino <= 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1; // failure
    }

    char *buf = (char *) fs_malloc(ioSize + 1);
    if (fs_allocated_write(ino, (void *) buf, ioSize) != ioSize) {
      fprintf(stderr, "fs_allocated_write() failed\n");
      fs_free(buf);
      return -1; // failure
    }

    fs_free(buf);
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("createfiles", elapsedTime);
  
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

  if (runWorkload(IO_SIZE, ITERATIONS) != 0) {
    fprintf(stderr, "runWorkload() failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
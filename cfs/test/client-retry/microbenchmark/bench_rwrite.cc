/*
* Writes to a file at random offsets
* Writes of size 1024
* Total data written = 128 MB
*/
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

// Macros
#define FILE_NAME "f0"
#define IO_SIZE 1024
#define ITERATIONS ((64) * (1000))
#define FILE_SIZE ((IO_SIZE) * (IO_SIZE))

// Function prototypes
int runWorkload(const char *path, ssize_t ioSize, ssize_t fileSize, ssize_t iter);

// Main
int runWorkload(const char *path, ssize_t ioSize, ssize_t fileSize, ssize_t iter) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  auto ino = fs_open(path, O_RDWR, 0);
  if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }

  char *buf = (char *) fs_malloc(ioSize + 1);
  int offset = 0;
  for (int i = 0; i < iter; i++) {
    memcpy(buf, generateString("a", ioSize).c_str(), ioSize);
    int offset = rand() % (fileSize - ioSize);
    if (fs_allocated_pwrite(ino, (void *) buf, ioSize, offset) != ioSize) {
      fprintf(stderr, "fs_allocated_pwrite() failed\n");
      fs_free(buf);
      return -1; // failure
    }
  }

  fs_free(buf);

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("rwrite", elapsedTime);
  
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

  if (runWorkload(FILE_NAME, IO_SIZE, FILE_SIZE, ITERATIONS) != 0) {
    fprintf(stderr, "runWorkload() failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
/*
* Reads from a file at random offset
* Reads data of size 1024
* Reads until 1 MB
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
#define FILE_SIZE ((IO_SIZE) * (IO_SIZE))
#define ITERATIONS 1024

// Function prototypes
int runWorkload(const char *path, ssize_t ioSize, ssize_t fileSize, ssize_t iter);

// Main
int runWorkload(const char *path, ssize_t ioSize, ssize_t fileSize, ssize_t iter) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  auto ino = fs_open(path, O_RDONLY, 0);
  if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }

  char *buf = (char *) fs_malloc(ioSize + 1);
  for (int i = 0; i < iter; i++) {
    int offset = rand() % (fileSize - ioSize);
    if (fs_allocated_pread(ino, (void *) buf, ioSize, offset) != ioSize) {
      fprintf(stderr, "fs_allocated_pread() failed at iter: %ld, offset: %d\n", iter, offset);
      fs_free(buf);
      return -1; // failure
    }
  }

  fs_free(buf);

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("rread", elapsedTime);
  
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
/*
* Appends to an empty file
* I/O size = [1B-8KB]
* Total write size = 1 MB
* fsync every 100 writes
*/
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

// Macros
#define FILE_NAME "f1"
#define ONE_KB 1024
#define DATA_WRITE ((ONE_KB) * (ONE_KB))
#define IO_SIZE_END_RANGE ((8) * (ONE_KB))

// Function prototypes
int runWorkload(const char *path);

// Main
int runWorkload(const char *path) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  auto ino = fs_open(path, O_RDWR, 0);
  if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }

  int countWritten = 0;
  int iteration = 0;
  while (countWritten < DATA_WRITE) {
    int ioSize = std::max(1, (rand() % IO_SIZE_END_RANGE));
    ioSize = (DATA_WRITE - countWritten) < ioSize ? (DATA_WRITE - countWritten) : ioSize;
    
    char *buf = (char *) fs_malloc(ioSize + 1);
    memcpy(buf, generateString("a", ioSize).c_str(), ioSize);
    
    if (fs_allocated_pwrite(ino, (void *) buf, 
      ioSize, std::max(countWritten - 1, 0)) != ioSize) {
      fprintf(stderr, "fs_allocated_pwrite() failed at ioSize = %d, countWritten = %d\n", ioSize, countWritten);
      fs_free(buf);
      return -1; // failure
    }

    fs_free(buf);

    if (iteration != 0 && iteration % 100 == 0) {
      if (fs_fsync(ino) != 0) {
        fprintf(stderr, "fs_fsync() failed at ioSize = %d, countWritten = %d\n", ioSize, countWritten);
        fs_free(buf);
        return -1; // failure
      }
    }

    countWritten += ioSize;
    iteration++;
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("seqwriterandsync", elapsedTime);
  
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

  if (runWorkload(FILE_NAME) != 0) {
    fprintf(stderr, "runWorkload() failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
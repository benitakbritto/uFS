// Create the FSPsrc dir here
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

// Macros
#define FILE_MB "f0"
#define ONE_KB 1024
#define ONE_MB (1024 * 1024)
#define NEW_FILE "f1"
#define CREAT_FILE_COUNT 50


// Function prototypes
int createOneMbFile();
int createFile(const char *fileName);
int createFiles(int count);
int runInit();

// Main
int runInit() {
  if (createOneMbFile() != 0) {
    fprintf(stderr, "createOneMbFile() failed\n");
    return -1; // failure
  }

  if (createFile(NEW_FILE) != 0) {
    fprintf(stderr, "createFile() failed\n");
    return -1; // failure
  }

  if (createFiles(CREAT_FILE_COUNT) != 0) {
    fprintf(stderr, "createFiles() failed\n");
    return -1; // failure
  }

  return 0;
}

int createOneMbFile() {
  auto ino = fs_open(FILE_MB, O_CREAT, 0644);
  if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }


  char *buf = (char *) fs_malloc(ONE_KB + 1);
  assert(buf != nullptr);
  memset(buf, 0, ONE_KB + 1);
  memcpy(buf, generateString("a", ONE_KB).c_str(), ONE_KB);
  
  int offset = 0;
  for (int i = 0; i < ONE_KB; i++) {
    if (fs_allocated_pwrite(ino, (void *) buf, ONE_KB, offset) != ONE_KB) {
      fprintf(stderr, "fs_allocated_write() failed\n");
      return -1; // failure
    }

    fs_syncall();
    offset += ONE_KB;
  }
  
  return 0; // success
}

int createFile(const char *fileName) {
   auto ino = fs_open(fileName, O_CREAT, 0644);
   if (ino <= 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1; // failure
  }

  return 0; // success
}

int createFiles(int count) {
  std::cout << "[INFO] Creating files" << std::endl;
  std::string parentPath = "";
  std::string currentPath = "";
  for (int i = 0; i < count; i++) {
    if (i % 10 == 0) {
      parentPath += "dir" + std::to_string(i / 10) + "/";
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
  }

  return 0;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage %s <pid1,pid2,..>\n", argv[0]);
    fprintf(stderr, "\t requires only two argument\n");
    fprintf(stderr,
            "\t Arg-1 --- pid1,pid2,..: a list of integers separated by , "
            "(comma). The last pid will be used for the pending queue.\n");
    exit(1);
  }

  if (initClient(argv[1]) != 0) {
    exit(1);
  }

  if (runInit() != 0) {
    exit(1);
  }

  fs_syncall();
  
  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
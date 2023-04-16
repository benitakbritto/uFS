// Create the FSPsrc dir here
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "fsapi.h"
#include "util.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define ONE_KB 1024
#define ONE_MB (1024 * 1024)
#define DIR_PREFIX "dir"
#define WRITE_FILE_PREFIX "f"
#define CREATE_FILE_PREFIX "appendlog"

/******************************************************************************
 * Prototypes
 *****************************************************************************/
int runInit(int numDirs, int numFilesPerDir, int fileSize);
void printUsage(char *executableName);

/******************************************************************************
 * DRIVER
 *****************************************************************************/

int runInit(int numDirs, int numFilesPerDir, int fileSize) {  
  for (int d = 0; d < numDirs; d++) {
    std::string dirName = DIR_PREFIX + std::to_string(d);
    if (fs_mkdir(dirName.c_str(), 0755) != 0) {
      fprintf(stderr, "fs_mkdir() failed\n");
      return -1; // failure
    }

    printf("[INFO] Created dir %s\n", dirName.c_str());

    for (int f = 0; f < numFilesPerDir; f++) {
      std::string fileName = WRITE_FILE_PREFIX + std::to_string(f);
      auto ino = fs_open((dirName + "/" + fileName).c_str(), O_CREAT, 0644);
      if (ino <= 0) {
        fprintf(stderr, "fs_open() failed\n");
        return -1; // failure
      }

      printf("[INFO] Created file %s\n", (dirName + "/" + fileName).c_str());

      char *buf = (char *) fs_malloc(ONE_KB + 1);
      assert(buf != nullptr);
      memset(buf, 0, ONE_KB + 1);
      memcpy(buf, generateString("a", ONE_KB).c_str(), ONE_KB);
    
      int offset = 0;
      int chunks = (fileSize  * ONE_MB) / ONE_KB;
      for (int i = 0; i < chunks; i++) {
        if (fs_allocated_pwrite(ino, (void *) buf, ONE_KB, i * ONE_KB) 
          != ONE_KB) {
          fprintf(stderr, 
            "fs_allocated_pwrite() failed at d = %d, f = %d and i = %d\n", 
            d, f, i);
          return -1; // failure
        }
      }

      printf("[INFO] Wrote to file %s\n", (dirName + "/" + fileName).c_str());

      fs_free(buf);
    }

    // Create empty file
    auto ino = fs_open((dirName + "/" + CREATE_FILE_PREFIX).c_str(), O_CREAT, 0644);
    if (ino <= 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1; // failure
    }

    printf("[INFO] Created file %s\n", (dirName + "/appendlog").c_str());

    fs_syncall();
  }
 
  return 0; // success
}

void printUsage(char *executableName) {
  printf("Usage: %s -p <pid1, ..> -d <numDirs> -f <numFilesPerDir> -s <fileSize MB>",
    executableName);
}


int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int numDirs = 1;
  int numFilesPerDir = 1;
  int fileSize = 1;

  while ((c = getopt(argc, argv, "p:d:f:s:h")) != -1) {
    switch (c) {
      case 'p':
        pids = std::string(optarg);
        break;
      case 'd':
        numDirs = std::stoi(optarg);
        break;
      case 'f':
        numFilesPerDir = std::stoi(optarg);
        break;
      case 's':
        fileSize = std::stoi(optarg);
        break;
      case 'h':
        printUsage(argv[0]);
        exit(1);
        break;
      default:
        printUsage(argv[0]);
        exit(1);
    }
  }

  if (initClient(pids) != 0) {
    exit(1);
  }

  if (runInit(numDirs, numFilesPerDir, fileSize) != 0) {
    exit(1);
  }
  
  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
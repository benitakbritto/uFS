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
#define IO_SIZE (4 * ONE_KB)
#define ONE_MB (1024 * 1024)
#define DIR_PREFIX "dir"
#define READ_FILE_PREFIX "f"
#define WRITE_FILE_PREFIX "appendlog"

/******************************************************************************
 * GLOBALS
 *****************************************************************************/
enum Workload {
  WEBSERVER, // 0
  VARMAIL, // 1
};

/******************************************************************************
 * Prototypes
 *****************************************************************************/
int runInitWebserver(int numDirs, int numFilesPerDir, int fileSize);
int runInitVarmail(int numDirs, int numFilesPerDir, int fileSize);
int runInit(int type, int numDirs, int numFilesPerDir, int fileSize);
void printUsage(char *executableName);

/******************************************************************************
 * DRIVER
 *****************************************************************************/

int runInitWebserver(int numDirs, int numFilesPerDir, int fileSize) {  
  if (fs_opendir(".") == nullptr) {
    return -1;
  }

  for (int d = 0; d < numDirs; d++) {
    std::string dirName = DIR_PREFIX + std::to_string(d);
    if (fs_mkdir(dirName.c_str(), 0755) != 0) {
      fprintf(stderr, "fs_mkdir() failed\n");
      return -1; // failure
    }

    printf("[INFO] Created dir %s\n", dirName.c_str());

    for (int f = 0; f < numFilesPerDir; f++) {
      std::string fileName = READ_FILE_PREFIX + std::to_string(f);
      auto ino = fs_open((dirName + "/" + fileName).c_str(), O_CREAT, 0644);
      if (ino <= 0) {
        fprintf(stderr, "fs_open() failed\n");
        return -1; // failure
      }

      printf("[INFO] Created file %s\n", (dirName + "/" + fileName).c_str());

      char *buf = (char *) fs_malloc(IO_SIZE + 1);
      assert(buf != nullptr);
      memset(buf, 0, IO_SIZE + 1);
      memcpy(buf, generateString("a", IO_SIZE).c_str(), IO_SIZE);
    
      int offset = 0;
      int chunks = (fileSize  * ONE_KB) / IO_SIZE;
      for (int i = 0; i < chunks; i++) {
        if (fs_allocated_pwrite(ino, (void *) buf, IO_SIZE, i * IO_SIZE) 
          != IO_SIZE) {
          fprintf(stderr, 
            "fs_allocated_pwrite() failed at d = %d, f = %d and i = %d\n", 
            d, f, i);
          return -1; // failure
        }
      }

      printf("[INFO] Wrote to file %s\n", (dirName + "/" + fileName).c_str());
      fs_free(buf);

      // Create empty files
      // auto inoCreat = fs_open((dirName + "/" + WRITE_FILE_PREFIX + std::to_string(f)).c_str(), 
      //   O_CREAT, 0644);
      // if (inoCreat <= 0) {
      //   fprintf(stderr, "fs_open() failed\n");
      //   return -1; // failure
      // }

      // printf("[INFO] Created file %s\n", (dirName + "/" + WRITE_FILE_PREFIX + std::to_string(f)).c_str());
    }

    fs_syncall();
  }
 
  return 0; // success
}

int runInitVarmail(int numDirs, int numFilesPerDir, int fileSize) {
  for (int d = 0; d < numDirs; d++) {
    std::string dirName = DIR_PREFIX + std::to_string(d);
    if (fs_mkdir(dirName.c_str(), 0755) != 0) {
      fprintf(stderr, "fs_mkdir() failed\n");
      return -1; // failure
    }

    printf("[INFO] Created dir %s\n", dirName.c_str());

    for (int f = 0; f < numFilesPerDir; f++) {
      std::string fileName = READ_FILE_PREFIX + std::to_string(f);
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
  }

  return 0;
}

int runInit(int type, int numDirs, int numFilesPerDir, int fileSize) {
  switch (type) {
    case Workload::WEBSERVER:
      return runInitWebserver(numDirs, numFilesPerDir, fileSize);
    case Workload::VARMAIL:
      return runInitVarmail(numDirs, numFilesPerDir, fileSize);
  }

  return 0;
}

void printUsage(char *executableName) {
  printf("Usage: %s -p <pid1, ..> -d <numDirs> -f <numFilesPerDir>" \
    "-s <fileSize MB> -t <0: webserver, 1: varmail> \n",
    executableName);
}

int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int numDirs = 1;
  int numFilesPerDir = 1;
  int fileSize = 1;
  int type = 0;

  while ((c = getopt(argc, argv, "p:d:f:s:t:h")) != -1) {
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
      case 't':
        type = std::stoi(optarg);
        assert(type == 0 || type == 1);
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

  if (runInit(type, numDirs, numFilesPerDir, fileSize) != 0) {
    exit(1);
  }
  
  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
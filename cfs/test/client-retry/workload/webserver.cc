#include <assert.h>
#include <fcntl.h>
#include <future>
#include "util.h"
#include "../../client/testClient_common.h"
#include <fsapi.h>
#include <sstream>
#include "common.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define DIR_PREFIX "dir"
#define READ_FILE_PREFIX "f"
#define WRITE_FILE_PREFIX "appendlog"
#define ONE_KB 1024
#define ONE_MB (ONE_KB * ONE_KB)
#define WRITE_SIZE_PER_FILE (5 * ONE_KB)

/******************************************************************************
 * GLOBALS
 *****************************************************************************/
enum TestCase {
  READ_ONLY, // 0
  READ_WRITE, // 1
};

/******************************************************************************
 * PROTOTYPES
 *****************************************************************************/
std::string getReadFilePath(int index, int numFilesPerDir);
std::string getWriteFilePath(int index, int numFilesPerDir);
int getTotalNumOfFiles(int numDirs, int numFilesPerDir);
int getNumFilesPerThread(int numThreads, int numDirs, int numFilesPerDir);
int getReadFileInode(int index, int numFilesPerDir);
int getWriteFileInode(int index, int numFilesPerDir);
int readFile(int ino, int fileSize, int ioSize);
int closeFile(int ino);
int appendToFile(int index, int numFilesPerDir, int ioSize);
int callOpen(std::string path);
void printUsage(char *executableName);

int runTask(int startIndex, int endIndex, int numFilesPerDir, 
  int fileSize, int ioSize, int type);
int runWorkload(int numThreads, int numDirs, int numFilesPerDir, int fileSize, 
  int ioSize, int type);

/******************************************************************************
 * DRIVER
 *****************************************************************************/

// Assumption: equal number of files in every directory
std::string getReadFilePath(int index, int numFilesPerDir) {
  int dirNum = index / numFilesPerDir;
  int fileNum = index % numFilesPerDir;

  return DIR_PREFIX + std::to_string(dirNum) 
    + "/" + READ_FILE_PREFIX + std::to_string(fileNum); 
}

std::string getWriteFilePath(int index, int numFilesPerDir) {
  int dirNum = index / numFilesPerDir;
  int fileNum = index % numFilesPerDir;

  return DIR_PREFIX + std::to_string(dirNum) 
    + "/" + WRITE_FILE_PREFIX + std::to_string(fileNum); 
}

int getTotalNumOfFiles(int numDirs, int numFilesPerDir) {
  return numDirs * numFilesPerDir;
}

int getNumFilesPerThread(int numThreads, int numDirs, 
  int numFilesPerDir) {
  return getTotalNumOfFiles(numDirs, numFilesPerDir) / numThreads;
}

int callOpen(std::string path) {
  auto ino = fs_open(path.c_str(), O_RDWR, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed for file %s\n", path.c_str());
    return -1;
  }

  return ino;
}

int getReadFileInode(int index, int numFilesPerDir) {
  std::string path = getReadFilePath(index, numFilesPerDir);
  return callOpen(path);
}

int getWriteFileInode(int index, int numFilesPerDir) {
  std::string path = getWriteFilePath(index, numFilesPerDir);
  return callOpen(path);
}

int readFile(int ino, int fileSize, int ioSize) {
  char *buf = (char *) fs_malloc(ioSize + 1);
  memset(buf, 0, ioSize + 1);
  
  int iterations = (fileSize * ONE_MB) / ioSize;
  for (int j = 0; j < iterations; j++) {
    auto ret = fs_allocated_read(ino, buf, ioSize);
    if (ret != ioSize) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", 
        ret);
      return -1;
    }
  }

  fs_free(buf);
  return 0;
}

int closeFile(int ino) {
  if (fs_close(ino) != 0) {
    fprintf(stderr, "fs_close() failed\n");
    return -1;
  } 

  return 0;
}

int appendToFile(int index, int numFilesPerDir, int ioSize) {
  int ino = getWriteFileInode(index, numFilesPerDir);

  int iterations = WRITE_SIZE_PER_FILE / ioSize;
  char *buf = (char *) fs_malloc(ioSize + 1);
  assert(buf != nullptr);
  memset(buf, 0, ioSize + 1);
  memcpy(buf, generateString("a", ioSize).c_str(), ioSize);

  for (int i = 0; i < iterations; i++) {
    if (fs_allocated_write(ino, buf, ioSize) != ioSize) {
      fprintf(stderr, "fs_allocated_write() failed.\n");
      return -1;
    } 
  }  
  
  return 0;
}

int runTask(int startIndex, int endIndex, int numFilesPerDir, 
  int fileSize, int ioSize, int type) {
  for (int i = startIndex; i < endIndex; i++) {
    int ino = getReadFileInode(i, numFilesPerDir);
    if (ino == 0) {
      return -1;
    }

    // Read whole file in IO_SIZE chunks
    if (readFile(ino, fileSize, ioSize) == -1) {
      return -1;
    }

    if (closeFile(ino) == -1) {
      return -1;
    }
  
    if (type == TestCase::READ_WRITE) {
      if (appendToFile(i, numFilesPerDir, ioSize) == -1) {
        return -1;
      }
    }
  
  }

  return 0;
}

int runWorkload(int numThreads, int numDirs, 
  int numFilesPerDir, int fileSize, int ioSize, int type) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::future<int> workers[numThreads];

  int filesPerThread = getNumFilesPerThread(numThreads, numDirs, numFilesPerDir);

  for (int i = 0; i < numThreads; i++) {
    workers[i] = std::async(
      runTask, 
      filesPerThread * i /*start*/, 
      filesPerThread * (i + 1) /*end non-inclusive*/,
      numFilesPerDir, 
      fileSize,
      ioSize,
      type); 
  }

  for (int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      fprintf(stderr, "[ERR] runTask failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("webserver-ro", elapsedTime);

  return 0;
}

void printUsage(char *executableName) {
  printf("Usage: %s -p <pid1, ..> " \ 
    "-d <numDirs> -f <numFilesPerDir>" \
    "-s <fileSize MB> -i <ioSize B>" \
    "-t <0: ro, 1: rw>\n",
    executableName);
}

int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int numThreads = 1;
  int numDirs = 1;
  int numFilesPerDir = 1;
  int fileSize = 1;
  int ioSize = 1;
  int type = 1;
  while ((c = getopt(argc, argv, "p:n:f:s:i:t:h")) != -1) {
    switch (c) {
      case 'p':
        pids = std::string(optarg);
        break;
      case 'n':
        numThreads = std::stoi(optarg);
        break;
      case 'f':
        numFilesPerDir = std::stoi(optarg);
        break;
      case 's':
        fileSize = std::stoi(optarg);
        break;
      case 'i':
        ioSize = std::stoi(optarg);
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

  if (runWorkload(numThreads, numDirs, numFilesPerDir, 
    fileSize, ioSize, type) != 0) {
    fprintf(stderr, "runWorkload failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
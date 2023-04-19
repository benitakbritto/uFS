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
#define FILE_PREFIX "f"
#define ONE_KB 1024
#define ONE_MB (ONE_KB * ONE_KB)
#define FILES_PER_DIR 4
#define TOTAL_DIRS 6

/******************************************************************************
 * PROTOTYPES
 *****************************************************************************/
int getTotalFiles();
int getDirsPerThread(int numThreads);
int getStartIndex(int threadIndex, int dirsPerThread);
std::string getFilePath(int index);
int callOpen(std::string path, bool create);
int getFileInode(int index, bool create = false);
int readFile(int ino, int readFileSize, int ioSize);
int closeFile(int ino);
int appendToFile(int index, int writeSize, int ioSize);
int deleteTask(int index);
int readOnlyTask(int index, int readFileSize, int ioSize);
int readWriteTask(int index, int readFileSize, int ioSize, int writeSize);
int createWriteTask(int index, int ioSize, int writeSize);
int runTask(int startIndex, int repeat, int readFileSize, int writeFileSize, int ioSize);
int runWorkload(int numThreads, int readFileSize, int writeFileSize, int ioSize);

/******************************************************************************
 * DRIVER
 *****************************************************************************/
int getTotalFiles() {
  return TOTAL_DIRS * FILES_PER_DIR;
}

int getDirsPerThread(int numThreads) {
  return TOTAL_DIRS / numThreads;
}

int getStartIndex(int threadIndex, int dirsPerThread) {
  return ((TOTAL_DIRS / dirsPerThread) * FILES_PER_DIR) * threadIndex;
}

// Assumption: equal number of files in every directory
std::string getFilePath(int index) {
  int dirNum = index / FILES_PER_DIR;
  int fileNum = index % FILES_PER_DIR;

  return DIR_PREFIX + std::to_string(dirNum) 
    + "/" + FILE_PREFIX + std::to_string(fileNum); 
}

int callOpen(std::string path, bool create) {
  int flags = create ? O_CREAT : O_RDWR;
  auto ino = fs_open(path.c_str(), flags, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed for file %s\n", path.c_str());
    return -1;
  }

  return ino;
}

int getFileInode(int index, bool create) {
  std::string path = getFilePath(index);
  return callOpen(path, create);
}

int readFile(int ino, int readFileSize, int ioSize) {
  char *buf = (char *) fs_malloc(ioSize + 1);
  memset(buf, 0, ioSize + 1);
  
  int iterations = (readFileSize * ONE_MB) / ioSize;
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

int appendToFile(int index, int writeSize, int ioSize) {
  int ino = getFileInode(index);

  int iterations = writeSize;
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

int deleteTask(int index) {
  std::string path = getFilePath(index);
  if (fs_unlink(path.c_str()) != 0) {
    fprintf(stderr, "fs_unlink() failed\n");
    return -1;
  }

  return 0;
}

int readOnlyTask(int index, int readFileSize, int ioSize) {
  auto ino = getFileInode(index);
  assert(readFile(ino, readFileSize, ioSize) == 0);
  assert(closeFile(ino));
  
  return 0;
}

int readWriteTask(int index, int readFileSize, int ioSize, int writeSize) {
  auto ino = getFileInode(index);
  assert(readFile(ino, readFileSize, ioSize) == 0);
  assert(appendToFile(ino, writeSize, ioSize) == 0);
  fs_syncall();
  assert(closeFile(ino));

  return 0;
}

int createWriteTask(int index, int ioSize, int writeSize) {
  auto ino = getFileInode(index, true /*create*/);
  assert(appendToFile(ino, writeSize, ioSize) == 0);
  fs_syncall();
  assert(closeFile(ino));

  return 0;
}

int runTask(int startIndex, int repeat, int readFileSize, int writeFileSize, int ioSize) {
  int ret = 0;

  for (int i = 0; i < repeat; i++) {
    ret = deleteTask(startIndex++);
    assert(ret == 0);

    ret = readOnlyTask(startIndex++, readFileSize, ioSize);
    assert(ret == 0);

    ret = readWriteTask(startIndex++, readFileSize, ioSize, writeFileSize);
    assert(ret = 0);

    ret = createWriteTask(startIndex++, ioSize, writeFileSize);
    assert(ret == 0);
  }

  return 0;
}

int runWorkload(int numThreads, int readFileSize, int writeFileSize, int ioSize) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::future<int> workers[numThreads];

  int repeatPerThread = getDirsPerThread(numThreads);

  for (int i = 0; i < numThreads; i++) {
    workers[i] = std::async(
      runTask, 
      getStartIndex(i, repeatPerThread),
      repeatPerThread, 
      readFileSize,
      writeFileSize,
      ioSize); 
  }

  for (int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      fprintf(stderr, "[ERR] runTask failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
  printElapsedTime("varmail", elapsedTime);

  return 0;
}

void printUsage(char *executableName) {
  printf("Usage: %s -p <pid1, ..> " \ 
    "-r <read size MB> -w <write size KB>" \
    "-i <ioSize B>\n",
    executableName);
}

int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int numThreads = 1;
  int readFileSize = 1;
  int writeFileSize = 1;
  int ioSize = 1;

  while ((c = getopt(argc, argv, "p:n:r:w:i:h")) != -1) {
    switch (c) {
      case 'p':
        pids = std::string(optarg);
        break;
      case 'n':
        numThreads = std::stoi(optarg);
        break;
      case 'r':
        readFileSize = std::stoi(optarg);
        break;
      case 'w':
        writeFileSize = std::stoi(optarg);
        break;
      case 'i':
        ioSize = std::stoi(optarg);
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

  if (runWorkload(numThreads, readFileSize, writeFileSize, ioSize) != 0) {
    fprintf(stderr, "runWorkload failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
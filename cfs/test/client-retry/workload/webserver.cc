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
std::string getReadFilePath(int index, int threadId);
std::string getWriteFilePath(int index, int threadId);
int callOpen(std::string path);
int getReadFileInode(int index, int threadId);
int getWriteFileInode(int index, int threadId);
int readFile(int index, int fileSize, int ioSize, int threadId);
int overwriteFile(int index, int fileSize, int ioSize, int threadId);
int closeFile(int ino);
int runTask(int threadId, int numFilesPerDir, int readFileSize, 
  int writeFileSize, int ioSize, int type);
int runWorkload(int numThreads, int numFilesPerDir, int readFileSize, 
  int writeFileSize, int ioSize, int type);
/******************************************************************************
 * DRIVER
 *****************************************************************************/

std::string getReadFilePath(int index, int threadId) {
  return DIR_PREFIX + std::to_string(threadId) 
    + "/" + READ_FILE_PREFIX + std::to_string(index); 
}

std::string getWriteFilePath(int index, int threadId) {
  return DIR_PREFIX + std::to_string(threadId) 
    + "/" + WRITE_FILE_PREFIX + std::to_string(index); 
}

int callOpen(std::string path) {
  auto ino = fs_open(path.c_str(), O_RDWR, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed for file %s\n", path.c_str());
    return -1;
  }

  return ino;
}

int getReadFileInode(int index, int threadId) {
  std::string path = getReadFilePath(index, threadId);
  return callOpen(path);
}

int getWriteFileInode(int index, int threadId) {
  std::string path = getWriteFilePath(index, threadId);
  return callOpen(path);
}

int readFile(int index, int fileSize, int ioSize, int threadId) {
  int ino = getReadFileInode(index, threadId);
  if (ino == 0) {
    return -1;
  }

  char *buf = (char *) fs_malloc(ioSize + 1);
  memset(buf, 0, ioSize + 1);

  int iterations = (fileSize * ONE_MB) / ioSize;
  for (int j = 0; j < iterations; j++) {
    auto ret = fs_allocated_pread(ino, buf, ioSize, j * ioSize);
    if (ret != ioSize) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", 
        ret);
      return -1;
    }
  }

  // fs_free(buf);

  if (closeFile(ino) != 0) {
    return -1;
  }

  return 0;
}

int closeFile(int ino) {
  if (fs_close(ino) != 0) {
    fprintf(stderr, "fs_close() failed\n");
    return -1;
  } 

  return 0;
}

int overwriteFile(int index, int fileSize, int ioSize, int threadId) {
  int ino = getReadFileInode(index, threadId);

  int iterations = (fileSize * ONE_MB) / ioSize;

  char* buf = (char *) fs_malloc(ioSize + 1);
  memset(buf, 0, ioSize + 1);
  memcpy(buf, generateString("a", ioSize).c_str(), ioSize);
 
  for (int i = 0; i < iterations; i++) {
    // std::cout << "[DEBUG] i = " << i << std::endl;
    if (fs_allocated_pwrite(ino, buf, ioSize, i * ioSize) != ioSize) {
      fprintf(stderr, "fs_allocated_write() failed.\n");
      return -1;
    } 
  }  

  if (closeFile(ino) != 0) {
    return -1;
  }

  // fs_free(buf);
  
  return 0;
}

int runTask(int threadId, int numFilesPerDir, int readFileSize, 
  int writeFileSize, int ioSize, int type) {

  for (int i = 0; i < numFilesPerDir; i++) {
    // Read whole file in IO_SIZE chunks
    if (readFile(i, readFileSize, ioSize, threadId) == -1) {
      return -1;
    }

    if (type == TestCase::READ_WRITE) {
      if (overwriteFile(i, writeFileSize, ioSize, threadId) == -1) {
        return -1;
      }
    }
  
  }

  return 0;
}

int runWorkload(int numThreads, int numFilesPerDir, int readFileSize, 
  int writeFileSize, int ioSize, int type) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    workers[i] = std::async(
      runTask, 
      i,
      numFilesPerDir,
      readFileSize,
      writeFileSize,
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
    "-f <numFilesPerDir> -r <read fileSize MB>" \
    "-w <write fileSize MB> -i <ioSize B>" \
    "-t <0: ro, 1: rw>\n",
    executableName);
}

int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int numThreads = 1;
  int numFilesPerDir = 1;
  int readFileSize = 1;
  int writeFileSize = 1;
  int ioSize = 1;
  int type = 1;

  while ((c = getopt(argc, argv, "p:n:f:r:w:i:t:h")) != -1) {
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
      case 'r':
        readFileSize = std::stoi(optarg);
        break;
      case 'w':
        writeFileSize = std::stoi(optarg);
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

  if (runWorkload(numThreads, numFilesPerDir, readFileSize, 
    writeFileSize, ioSize, type) != 0) {
    fprintf(stderr, "runWorkload failed\n");
    exit(1);
  }

  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  
  return 0;
}
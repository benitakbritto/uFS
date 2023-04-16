// TODO: Update the pending count since the addition of the threshold
#include <assert.h>
#include <fcntl.h>
#include <future>
#include "util.h"
#include "../../../client/testClient_common.h"
#include <fsapi.h>
#include <sstream>

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define RUN_COUNT 100
#define RING_SIZE 64
#define IO_SIZE 1024
#define STRESS_IO_SIZE (IO_SIZE * IO_SIZE)
#define shmipc_STATUS_EMPTY 0
#define shmipc_STATUS_RESERVED 1
#define shmipc_STATUS_READY_FOR_SERVER 2
#define shmipc_STATUS_IN_PROGRESS 3
#define shmipc_STATUS_READY_FOR_CLIENT 4
#define shmipc_STATUS_NOTIFY_FOR_CLIENT 5
#define shmipc_STATUS_SERVER_PID_FOR_CLIENT 6

/******************************************************************************
 * GLOBALS
 *****************************************************************************/
enum TestCase
{
    OPEN_DIR, // 0
    OPEN, // 1
    CREATE, // 2
    ALLOC_READ, // 3
    ALLOC_WRITE, // 4
    ALLOC_PREAD, // 5
    ALLOC_PWRITE, // 6
    CLOSE, // 7
    MKDIR, // 8
    STAT, // 9
    FSTAT, // 10
    UNLINK, // 11
    FSYNC, // 12
    STRESS_MALLOC, // 13
    OPEN_DIR_THREADS, // 14
    OPEN_THREADS, // 15
    CREATE_THREADS, // 16
    ALLOC_READ_THREADS, // 17
    ALLOC_WRITE_THREADS, // 18
    ALLOC_PREAD_THREADS, // 19
    ALLOC_PWRITE_THREADS, // 20
};

/******************************************************************************
 * PROTOTYPES
 *****************************************************************************/
std::string generateString(const std::string str1, int repeatCount);
int initClient(const std::string &pids);
int countEmptySlots(std::vector<int> &ringBuffer);
int runOpenDir();
int runOpen();
int runCreate();
int runAllocRead();
int runAllocWrite();
int runAllocPread();
int runAllocPwrite();
int runClose();
int runMkdir();
int runStat();
int runFstat();
int runUnlink();
int runFsync();
int runStressMalloc();
void printHelp(const char *executableName);

int runOpenDirSingle(const char *path);
int runOpenDirWithThreads(int numThreads);
int runOpenSingle(const char *path);
int runOpenWithThreads(int numThreads);
int runCreateSingle(std::string dirPath);
int runCreateWithThreads(int numThreads);
int runAllocReadSingle(std::string path);
int runAllocReadWithThreads(int numThreads);
int runAllocWriteSingle(std::string path);
int runAllocWriteWithThreads(int numThreads);
int runAllocPreadSingle(std::string path);
int runAllocPreadWithThreads(int numThreads);
int runAllocPwriteSingle(std::string path);
int runAllocPwriteWithThreads(int numThreads);


/******************************************************************************
 * DRIVER
 *****************************************************************************/
std::string generateString(const std::string str1, int repeatCount) {
  std::ostringstream buffer;
  for (int i = 0; i < repeatCount; i++) {
    buffer << str1;
  }
  return buffer.str();
}

int initClient(const std::string &pids) {
  const int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  for (int i = 0; i < kMaxNumFsp; i++) shmKeys[i] = 0;
  auto pidStrVec = splitStr(pids, ',');
  int numKey = pidStrVec.size();
  assert(numKey <= kMaxNumFsp);

  for (int i = 0; i < numKey; i++) {
    shmKeys[i] = (FS_SHM_KEY_BASE) + stoiWrapper(pidStrVec[i]);
  }

  int rt = fs_init_multi(numKey, shmKeys);
  if (rt < 0) {
    fprintf(stderr, "fs_init_multi failure\n");
    return -1; // failure
  } else {
    fprintf(stdout, "fs_init_multi success\n");
  }

  fprintf(stdout, "[INFO] client connection successful\n");
  return 0; // success
}

int countEmptySlots(std::vector<int> &ringBuffer) {
  int count = 0;
  for (int i = 0; i < RING_SIZE; i++) {
    count += (ringBuffer[i] == shmipc_STATUS_EMPTY);
  }

  return count;
}

int runOpenDir() {
  for (int i = 0; i < RUN_COUNT; i++) {
    // Calls opendir on root
    auto dir = fs_opendir("/");
    if (dir == nullptr) {
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    if (pendingOpsListSize != 0) {
      return -1;
    }
  }
  
  return 0;
}

int runOpen() {
  for (int i = 0; i < RUN_COUNT; i++) {
    auto ino = fs_open("db", O_RDONLY, 0);
    if (ino == 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    if (pendingOpsListSize != 0) {
      fprintf(stderr, "incorrect pending ops\n");
      return -1;
    }
  }
  
  return 0;
}

int runCreate() {
  auto ret = fs_mkdir("test", 0);
  if (ret != 0) {
    fprintf(stderr, "fs_mkdir() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    auto ino = fs_open(("test/" + std::to_string(i)).c_str(), O_CREAT, 0644);
    if (ino == 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1;
    }

    // Check (main) ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = 0;
    switch(i) {
      case 62:
        expectedCount = 1;
        break;
      case 63:
        expectedCount = 2;
        break;
      case 64:
        expectedCount = 3;
        break;
      default:
        expectedCount = i + 2;
        break;
    }

    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld\n", pendingOpsListSize);
      return -1;
    }
  }
  
  return 0;
}

int initAllocRead(std::string path) {
  std::cout << "[DEBUG] Inside " << __func__ << std::endl;
  auto ino = fs_open(path.c_str(), O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  char *buf = (char *) fs_malloc(IO_SIZE * RUN_COUNT + 1);
  memset(buf, 0, IO_SIZE * RUN_COUNT + 1);
  auto ret = fs_allocated_pwrite(ino, buf, IO_SIZE * RUN_COUNT, 0);
  if (ret != IO_SIZE * RUN_COUNT) {
    fprintf(stderr, "fs_allocated_write() failed. Received %ld\n", ret);
    return -1;
  }

  fs_free(buf);

  fs_syncall();
  
  return 0;
}

int runAllocRead() {
  initAllocRead("db");

  auto ino = fs_open("db", O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *buf = (char *)fs_malloc(IO_SIZE + 1);
    memset(buf, 0, IO_SIZE + 1);
    auto ret = fs_allocated_read(ino, buf, IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", ret);
      return -1;
    }

    // Check (main) ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = 1; // from the init
    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld for i %d\n", pendingOpsListSize, i);
      return -1;
    }

    fs_free(buf);
  }
  
  return 0;
}

int runAllocWrite() {
  auto ino = fs_open("db", O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *bufWrite = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufWrite, 0, IO_SIZE + 1);
    memcpy(bufWrite, generateString("a", IO_SIZE).c_str(), IO_SIZE);

    auto ret = fs_allocated_write(ino, bufWrite, IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_write() failed. Received %ld\n", ret);
      return -1;
    }

    char *bufRead = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufRead, 0, IO_SIZE + 1);
    ret = fs_allocated_pread(ino, bufRead, IO_SIZE, i * IO_SIZE);
    if (strcmp(bufRead, bufWrite) != 0) {
      fprintf(stderr, "read and write bufs are not the same\n");
      return -1;
    }

    // Check (main) ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = (i % 63) + 1;
    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld for i %d\n", pendingOpsListSize, i);
      return -1;
    }
  
    fs_free(bufRead);
  }
  
  return 0;
}

int runAllocPread() {
  initAllocRead("db");

  auto ino = fs_open("db", O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *buf = (char *)fs_malloc(IO_SIZE + 1);
    memset(buf, 0, IO_SIZE + 1);
    auto ret = fs_allocated_pread(ino, buf, IO_SIZE, i * IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", ret);
      return -1;
    }

    // Check (main) ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = 1; // from the init
    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld for i %d\n", pendingOpsListSize, i);
      return -1;
    }
  
    fs_free(buf);
  }
  
  return 0;
}

int runAllocPwrite() {
  auto ino = fs_open("db", O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *bufWrite = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufWrite, 0, IO_SIZE + 1);
    memcpy(bufWrite, generateString("a", IO_SIZE).c_str(), IO_SIZE);

    auto ret = fs_allocated_pwrite(ino, bufWrite, IO_SIZE, IO_SIZE * i);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_pwrite() failed. Received %ld\n", ret);
      return -1;
    }

    char *bufRead = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufRead, 0, IO_SIZE + 1);
    ret = fs_allocated_pread(ino, bufRead, IO_SIZE, i * IO_SIZE);
    if (strcmp(bufRead, bufWrite) != 0) {
      fprintf(stderr, "read and write bufs are not the same\n");
      return -1;
    }

    // Check (main) ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = (i % 63) + 1;
    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld for i %d\n", pendingOpsListSize, i);
      return -1;
    }
  
    fs_free(bufRead);
  }
  
  return 0;
} 

int runClose() {
    for (int i = 0; i < RUN_COUNT; i++) {
    auto ino = fs_open("db", O_RDONLY, 0);
    if (ino == 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1;
    }

    auto ret = fs_close(ino);
    if (ret != 0) {
      fprintf(stderr, "fs_close() failed\n");
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    if (pendingOpsListSize != 0) {
      fprintf(stderr, "incorrect pending ops\n");
      return -1;
    }
  }
  
  return 0;
}

int runMkdir() {
  for (int i = 0; i < RUN_COUNT; i++) {
    auto ret = fs_mkdir(std::to_string(i).c_str(), 0);
    if (ret != 0) {
      fprintf(stderr, "fs_mkdir() failed\n");
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    int expectedCount = 0;
    switch(i) {
      case 63:
        expectedCount = 1;
        break;
      case 64:
        expectedCount = 2;
        break;
      default:
        expectedCount = i + 1;
        break;
    }

    if (pendingOpsListSize != expectedCount) {
      fprintf(stderr, "incorrect pending ops. Received %ld\n", pendingOpsListSize);
      return -1;
    }
  }
  
  return 0;
}

int runStat() {
  for (int i = 0; i < RUN_COUNT; i++) {
    struct stat statbuf;
    auto ret = fs_stat("db", &statbuf);
    if (ret != 0) {
      fprintf(stderr, "fs_stat() failed\n");
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    if (pendingOpsListSize != 0) {
      fprintf(stderr, "incorrect pending ops\n");
      return -1;
    }
  }
  
  return 0;
}

int runFstat() {
  for (int i = 0; i < RUN_COUNT; i++) {
    struct stat statbuf;
    auto ret = fs_fstat(2, &statbuf);
    if (ret != 0) {
      fprintf(stderr, "fs_fstat() failed\n");
      return -1;
    }

    // Check ring buffer
    auto ringBuffer = fs_dump_ring_status();
    int emptyCount = countEmptySlots(ringBuffer);
    // 1 slot is reserved for server pid at all times
    if (emptyCount != (RING_SIZE - 1)) {
      fprintf(stderr, "empty slots do not match\n");
      return -1;
    }

    // Check pending
    auto pendingOpsListSize = fs_dump_pendingops().size();
    if (pendingOpsListSize != 0) {
      fprintf(stderr, "incorrect pending ops\n");
      return -1;
    }
  }
  
  return 0;
}

// TODO
int runUnlink() {
  return 0;
}

// TODO
int runFsync() {
  return 0;
}

int runStressMalloc() {
  auto ino = fs_open("db", O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *bufWrite = (char *) fs_malloc(STRESS_IO_SIZE + 1);
    if (bufWrite == nullptr) {
      printf("[DEBUG] fs_malloc was unsuccessful at iteration %d\n", i);
      return -1;
    }

    memcpy(bufWrite, generateString("a", STRESS_IO_SIZE).c_str(), STRESS_IO_SIZE);
    auto ret = fs_allocated_pwrite(ino, bufWrite, STRESS_IO_SIZE, STRESS_IO_SIZE * i);
    if (ret != STRESS_IO_SIZE) {
      fprintf(stderr, "fs_allocated_pwrite() failed. Received %ld\n", ret);
      return -1;
    }
  }

  return 0;
}

int runOpenDirSingle(const char *path) {
  for (int i = 0; i < RUN_COUNT; i++) {
    // Calls opendir on root
    auto dir = fs_opendir(path);
    if (dir == nullptr) {
      return -1;
    }
  }
  
  return 0;
}

int runOpenDirWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    workers[i] = std::async(runOpenDirSingle, "/"); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runOpenDirWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

int runOpenSingle(const char *path) {
  for (int i = 0; i < RUN_COUNT; i++) {
    // Calls opendir on root
    auto ino = fs_open(path, O_RDWR, 0);
    if (ino == 0) {
      return -1;
    }
  }
  
  return 0;
}

int runOpenWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  // cannot support more than 9 threads (since only t9 exists)
  for (int i = 0; i < numThreads; i++) {
    std::string path = "t" + std::to_string(i + 1);
    workers[i] = std::async(runOpenSingle, path.c_str()); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runOpenDirWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

// dirPath without / at the end
int runCreateSingle(std::string dirPath) {
  auto ret = fs_mkdir(dirPath.c_str(), 0);
  if (ret != 0) {
    fprintf(stderr, "fs_mkdir() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    auto ino = fs_open((dirPath + "/" + std::to_string(i)).c_str(), O_CREAT, 0644);
    if (ino == 0) {
      fprintf(stderr, "fs_open() failed\n");
      return -1;
    }
  }
  
  return 0;
} 

int runCreateWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    std::string path = "test" + std::to_string(i);
    workers[i] = std::async(runCreateSingle, path); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runCreateWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

int runAllocReadSingle(std::string path) {
  std::cout << "[DEBUG] Inside " << __func__ << std::endl;
  initAllocRead(path);

  auto ino = fs_open(path.c_str(), O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *buf = (char *) fs_malloc(IO_SIZE + 1);
    memset(buf, 0, IO_SIZE + 1);
    auto ret = fs_allocated_read(ino, buf, IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", ret);
      return -1;
    }

    fs_free(buf);
  }

  return 0;
}

int runAllocReadWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    std::string path = "t" + std::to_string(i + 1);
    workers[i] = std::async(runAllocReadSingle, path); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runAllocPreadWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

int runAllocWriteSingle(std::string path) {
  auto ino = fs_open(path.c_str(), O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *bufWrite = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufWrite, 0, IO_SIZE + 1);
    memcpy(bufWrite, generateString("a", IO_SIZE).c_str(), IO_SIZE);

    auto ret = fs_allocated_write(ino, bufWrite, IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_write() failed. Received %ld\n", ret);
      return -1;
    }

    char *bufRead = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufRead, 0, IO_SIZE + 1);
    ret = fs_allocated_pread(ino, bufRead, IO_SIZE, i * IO_SIZE);
    if (strcmp(bufRead, bufWrite) != 0) {
      fprintf(stderr, "read and write bufs are not the same\n");
      return -1;
    }

    fs_free(bufRead);
  }
  
  return 0;
}

int runAllocWriteWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    std::string path = "t" + std::to_string(i + 1);
    workers[i] = std::async(runAllocWriteSingle, path); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runAllocWriteWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

int runAllocPreadSingle(std::string path) {
  initAllocRead(path);

  auto ino = fs_open(path.c_str(), O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *buf = (char *) fs_malloc(IO_SIZE + 1);
    memset(buf, 0, IO_SIZE + 1);
    auto ret = fs_allocated_pread(ino, buf, IO_SIZE, IO_SIZE * i);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_read() failed. Received %ld\n", ret);
      return -1;
    }

    fs_free(buf);
  }

  return 0;
}

int runAllocPreadWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    std::string path = "t" + std::to_string(i + 1);
    workers[i] = std::async(runAllocPreadSingle, path); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runAllocPreadWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

int runAllocPwriteSingle(std::string path) {
  auto ino = fs_open(path.c_str(), O_RDONLY, 0);
  if (ino == 0) {
    fprintf(stderr, "fs_open() failed\n");
    return -1;
  }

  for (int i = 0; i < RUN_COUNT; i++) {
    char *bufWrite = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufWrite, 0, IO_SIZE + 1);
    memcpy(bufWrite, generateString("a", IO_SIZE).c_str(), IO_SIZE);

    auto ret = fs_allocated_pwrite(ino, bufWrite, IO_SIZE, i * IO_SIZE);
    if (ret != IO_SIZE) {
      fprintf(stderr, "fs_allocated_write() failed. Received %ld\n", ret);
      return -1;
    }

    char *bufRead = (char *)fs_malloc(IO_SIZE + 1);
    memset(bufRead, 0, IO_SIZE + 1);
    ret = fs_allocated_pread(ino, bufRead, IO_SIZE, i * IO_SIZE);
    if (strcmp(bufRead, bufWrite) != 0) {
      fprintf(stderr, "read and write bufs are not the same\n");
      return -1;
    }

    fs_free(bufRead);
  }
  
  return 0;
}

int runAllocPwriteWithThreads(int numThreads) {
  std::future<int> workers[numThreads];

  for (int i = 0; i < numThreads; i++) {
    std::string path = "t" + std::to_string(i + 1);
    workers[i] = std::async(runAllocPwriteSingle, path); 
  }

  for(int i = 0; i < numThreads; i++) {
    if (workers[i].get() != 0) {
      printf("[ERR] runAllocPwriteWithThreads failed with worker %d\n", i);
      return workers[i].get();
    }
  }

  return 0;
}

void printHelp(const char *executableName) {
  fprintf(stderr, "Usage %s -p <pid1,pid2,..> -t <test case num> -n <num threads>\n", executableName);
  fprintf(stderr, "\t requires two arguments\n");
  fprintf(stderr,
          "\t Argument 1 --- pid1,pid2,..: a list of integers separated by , "
          "(comma). The last pid will be used for the pending queue.\n");
  fprintf(stderr,
          "\t Argument 2 --- test case num:\n");
  fprintf(stderr,
          "\t\t 0: fs_opendir()\n");
  fprintf(stderr,
          "\t\t 1: fs_open()\n");
  fprintf(stderr,
          "\t\t 2: fs_open(create)\n");        
  fprintf(stderr,
          "\t\t 3: fs_allocated_read()\n");
  fprintf(stderr,
          "\t\t 4: fs_allocated_write()\n");
  fprintf(stderr,
          "\t\t 5: fs_allocated_pread()\n");
  fprintf(stderr,
          "\t\t 6: fs_allocated_pwrite()\n");
  fprintf(stderr,
          "\t\t 7: fs_close()\n");
  fprintf(stderr,
          "\t\t 8: fs_mkdir()\n");
  fprintf(stderr,
          "\t\t 9: fs_stat()\n");
  fprintf(stderr,
          "\t\t 10: fs_fstat()\n"); 
  fprintf(stderr,
          "\t\t 11: fs_unlink()\n"); 
  fprintf(stderr,
          "\t\t 12: fs_fsync()\n");
  fprintf(stderr,
          "\t\t 13: stress fs_malloc()\n"); 
  fprintf(stderr,
          "\t\t 14: fs_opendir() with threads\n");
  fprintf(stderr,
          "\t\t 15: fs_open() with threads\n");
  fprintf(stderr,
          "\t\t 16: fs_open(create) with threads\n"); 
  fprintf(stderr,
          "\t\t 17: fs_allocated_read() with threads\n"); 
  fprintf(stderr,
          "\t\t 18: fs_allocated_write() with threads\n"); 
  fprintf(stderr,
          "\t\t 19: fs_allocated_pread() with threads\n");  
  fprintf(stderr,
          "\t\t 20: fs_allocated_pwrite() with threads\n");        
}

int main(int argc, char **argv) {
  // Get command line args
  char c = '\0';
  std::string pids = "";
  int testCaseNum = -1;
  int numThreads = 1;
  while ((c = getopt(argc, argv, "p:t:n:")) != -1) {
    switch (c) {
      case 'p':
        pids = std::string(optarg);
        break;
      case 't':
        testCaseNum = std::stoi(optarg);
        break;
      case 'n':
        numThreads = std::stoi(optarg);
        break;
      default:
        printf("Invalid arg\n");
        exit(1);
    }
  }

  if (initClient(pids) != 0) {
    exit(1);
  }

  switch(testCaseNum) {
    case TestCase::OPEN_DIR: {
      if (runOpenDir() != 0) {
        fprintf(stderr, "runOpenDir() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::OPEN: {
      if (runOpen() != 0) {
        fprintf(stderr, "runOpen() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::CREATE: {
      if (runCreate() != 0) {
        fprintf(stderr, "runCreate() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_READ: {
      if (runAllocRead() != 0) {
        fprintf(stderr, "runAllocRead() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_WRITE: {
      if (runAllocWrite() != 0) {
        fprintf(stderr, "runAllocWrite() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_PREAD: {
      if (runAllocPread() != 0) {
        fprintf(stderr, "runAllocPread() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_PWRITE: {
      if (runAllocPwrite() != 0) {
        fprintf(stderr, "runAllocPwrite() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::CLOSE: {
      if (runClose() != 0) {
        fprintf(stderr, "runClose() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::MKDIR: {
      if (runMkdir() != 0) {
        fprintf(stderr, "runMkdir() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::STAT: {
      if (runStat() != 0) {
        fprintf(stderr, "runStat() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::FSTAT: {
      if (runFstat() != 0) {
        fprintf(stderr, "runFstat() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::UNLINK: {
      if (runUnlink() != 0) {
        fprintf(stderr, "runUnlink() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::FSYNC: {
      if (runFsync() != 0) {
        fprintf(stderr, "runFsync() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::STRESS_MALLOC: {
      if (runStressMalloc() != 0) {
        fprintf(stderr, "runFsync() failed\n");
        exit(1);
      }
      break;
    } 
    case TestCase::OPEN_DIR_THREADS: {
      if (runOpenDirWithThreads(2 /*numThreads*/) != 0) {
        fprintf(stderr, "runOpenDirWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::OPEN_THREADS: {
      if (runOpenWithThreads(numThreads) != 0) {
        fprintf(stderr, "runOpenDirWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::CREATE_THREADS: {
      if (runCreateWithThreads(numThreads) != 0) {
        fprintf(stderr, "runCreateWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_READ_THREADS: {
      if (runAllocReadWithThreads(numThreads) != 0) {
        fprintf(stderr, "runAllocReadWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_WRITE_THREADS: {
      if (runAllocWriteWithThreads(numThreads) != 0) {
        fprintf(stderr, "runAllocWriteWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_PREAD_THREADS: {
      if (runAllocPreadWithThreads(numThreads) != 0) {
        fprintf(stderr, "runAllocPreadWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    case TestCase::ALLOC_PWRITE_THREADS: {
      if (runAllocPwriteWithThreads(numThreads) != 0) {
        fprintf(stderr, "runAllocPwriteWithThreads() failed\n");
        exit(1);
      }
      break;
    }
    default: {
      printHelp(argv[0]);
      break;
    }
  }
  
  // cleanup
  if (fs_exit() != 0) {
    fprintf(stderr, "exit failed\n");
  }
  return 0;
}
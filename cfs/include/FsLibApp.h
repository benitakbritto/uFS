#ifndef CFS_FSLIBAPP_H
#define CFS_FSLIBAPP_H

#include <atomic>
#include <condition_variable>
#include <list>
#include <shared_mutex>
#include <unordered_map>

#include "FsLibLease.h"
#include "FsLibMalloc.h"
#include "FsLibPageCache.h"
#include "FsLibShared.h"
#include "absl/container/flat_hash_map.h"
#include "fsapi.h"
#include "rbtree.h"
#include "shmipc/shmipc.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/reader_writer_lock.h"
#include "tbb/scalable_allocator.h"

//
// Only used in FsLib
//


typedef std::shared_mutex Lock;
typedef std::unique_lock<Lock>  WriteLock;
typedef std::shared_lock<Lock>  ReadLock;

// debug print flag
// #define _CFS_LIB_DEBUG_
// number of blocks (pages) that is used for each thread's cache
// set to 64 * 1024 blocks (i.e., 64 * 1024 * 4K = 256 M)
//#define FS_LIB_THREAD_CACHE_NUM_BLOCK (64 * 1024)
#define FS_LIB_THREAD_CACHE_NUM_BLOCK (10 * 64 * 1024)
// On/Off of using app buffer, assume always use this if using allocated_X API
// By default, enable APP BUF
#define FS_LIB_USE_APP_BUF (1)
#define REQUEST_ID_BASE 100000 

// INVARIANT: threadFsTid == 0: thread uninitialized
// Otherwise, threadFsTid >= 1
extern thread_local int threadFsTid;

class CommuChannelAppSide : public CommuChannel {
 public:
  CommuChannelAppSide(pid_t id, key_t shmKey);
  ~CommuChannelAppSide();
  int submitSlot(int sid);

 protected:
  void *attachSharedMemory(void);
  void cleanupSharedMemory(void);
};

// single block of file whose status is flag
#define FSLIB_APP_BLK_RCD_FLAG_DIRTY (0b00000001)
struct BlockRecordOfFile {
  // REQUIRED: alignedStartOffset is aligned with page/block size
  // start offset within the file (in bytes)
  off_t alignedStartOffset{0};
  // valid content length (in bytes)
  size_t count{0};
  // memory address of data
  // REQUIRED: corresponds to alignedStartOffset (strictly aligned)
  void *addr{nullptr};
  // status of this block
  uint8_t flag{0};
};

// The FileHandle is mainly designed for client cache
// Because One app might open a file for several times and get several FDs,
// But we only want to keep one copy of the data inside client cache
// Thus, the sharing granularity is each file
struct FileHandle {
  char fileName[MULTI_DIRSIZE];
  // this fileHandle's reference count
  // we do not want to delete this item if all the FD is closed
  // REQUIRED: modification needs to be done with fdFileHandleMapLock held
  int refCount;
  int id;
  FileHandleCacheReader cacheReader;
  tbb::reader_writer_lock lock_;
};

class FsService {
 public:
  FsService();
  FsService(int wid, key_t key);
  ~FsService();
  void initService();
  int allocRingSlot();
  void releaseRingSlot(int slotId);
  // get the corresponding buffer associated with one slot
  // if reset is set to true, will init the memory buffer to zeros
  void *getSlotDataBufPtr(int slotId, bool reset);
  struct clientOp *getSlotOpPtr(int slotId);
  int submitReq(int slotId);

  key_t GetShmkey() { return shmKey; }

  struct shmipc_mgr *shmipc_mgr = NULL;
  bool inUse = false;

  void updateOffset(ino_t inode, off_t offset) {
    inodeOffsetMap[inode] += offset;
  }

  void setOffset(ino_t inode, off_t offset) {
    inodeOffsetMap[inode] = offset;
  }
  
  off_t getOffset(ino_t inode) {
    return inodeOffsetMap.count(inode) == 0 ? 0 : inodeOffsetMap[inode];
  }

 private:
  std::list<int> unusedRingSlots;
  std::atomic_flag unusedSlotsLock;
  key_t shmKey;
  int wid;
  CommuChannelAppSide *channelPtr;
  std::unordered_map<ino_t, off_t> inodeOffsetMap;
};

class RetryMgr {
public:
  RetryMgr() {
    this->detectServerAliveThread = std::thread(&RetryMgr::detectServerUnavailLoop, this);
  }

  // fs retries
  ssize_t fs_pwrite_retry(int fd, const void *buf, size_t count, off_t offset, uint64_t requestId);
  int fs_open_retry(const char *path, int flags, mode_t mode, uint64_t requestId);
  ssize_t fs_pread_retry(int fd, void *buf, size_t count, off_t offset, uint64_t requestId);
  int fs_unlink_retry(const char *pathname, uint64_t requestId);
  int fs_mkdir_retry(const char *pathname, mode_t mode, uint64_t requestId);
  int fs_stat_retry(const char *pathname, struct stat *statbuf, uint64_t requestId);
  int fs_fstat_retry(int fd, struct stat *statbuf, uint64_t requestId);
  CFS_DIR *fs_opendir_retry(const char *name, uint64_t requestId);
  int fs_fdatasync_retry(int fd, uint64_t requestId);
  int fs_close_retry(int fd, uint64_t requestId);
  int fs_fsync_retry(int fd, uint64_t requestId);
  ssize_t fs_allocated_pwrite_retry(int fd, void *buf, ssize_t count, off_t offset, uint64_t requestId);
  ssize_t fs_allocated_pread_retry(int fd, void *buf, size_t count, off_t offset, uint64_t requestId);

  // retry handler
  ssize_t handle_pwrite_retry(uint64_t reqId);
  ssize_t handle_allocated_pwrite_retry(uint64_t reqId);
  int handle_create_retry(uint64_t reqId);
  ssize_t handle_pread_retry(uint64_t reqId, void* buf);
  ssize_t handle_allocated_pread_retry(uint64_t reqId);
  int handle_unlink_retry(uint64_t reqId);
  int handle_mkdir_retry(uint64_t reqId);
  int handle_stat_retry(uint64_t reqId, struct stat* statbuf);
  int handle_fstat_retry(uint64_t reqId, struct stat* statbuf);
  int handle_open_retry(uint64_t reqId);
  int handle_close_retry(uint64_t reqId);
  int handle_opendir_retry(uint64_t reqId, CFS_DIR *dir);
  int handle_fsync_retry(uint64_t reqId);
  int handle_fdatasync_retry(uint64_t reqId);

  // main retry function
  int fs_retry_pending_ops(void *buf = nullptr, struct stat *statbuf = nullptr, 
    CFS_DIR *dir = nullptr);
  int fs_retry_pending_ops_for_thread(void *buf = nullptr, struct stat *statbuf = nullptr, 
    CFS_DIR *dir = nullptr, bool isBackground = false);

  void initNotifyShmForThread(int numThreads) {
    this->_notifyShmForThread = std::vector<bool>(numThreads, false);
  }

  int check_primary_server_availability();
  int get_server_pid();

  int getThreadIdFromRequestId(uint64_t requestId) {
    return requestId / REQUEST_ID_BASE;
  }

  // Ring Map
  // std::vector<uint64_t> getLeftoverRequestsToClean() {
  //   return std::vector<uint64_t>{this->_leftoverRequestToBeCleaned.begin(), 
  //     this->_leftoverRequestToBeCleaned.end()};
  // }

  // void clearLeftoverRequests() {
  //   this->_leftoverRequestToBeCleaned.clear();
  // }

  std::vector<off_t> getRingIdxsForRequest(uint64_t requestId) {    
    // ReadLock r_lock(_reqRingMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    ReadLock r_lock(*_reqRingMapLockList[threadId].get());
    if (_reqRingMap.count(requestId) == 0) {
      // std::cout << "[DEBUG] IMPORTANT request id does not exist in map" << std::endl; fflush(stdout);
      // _leftoverRequestToBeCleaned.insert(requestId);
    } else {
      // std::cout << "[DEBUG] list size is = " << _reqRingMap[requestId].size() << std::endl;
    }

    return _reqRingMap[requestId];
  }

  bool doesRequestIdExistInRingMap(uint64_t requestId) {
    // ReadLock r_lock(_reqRingMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    ReadLock r_lock(*_reqRingMapLockList[threadId].get());
    return _reqRingMap.count(requestId) != 0;
  }

  // Assumes requestId exists
  off_t getFirstRingIdxForRequest(uint64_t requestId) {
    // ReadLock r_lock(_reqRingMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    ReadLock r_lock(*_reqRingMapLockList[threadId].get());
    return _reqRingMap[requestId][0];
  }

  void removeRequestFromRingMap(uint64_t requestId) {
    if (_reqRingMap.count(requestId) != 0) {
      // std::cout << "[DEBUG] Deleted requestId = " << requestId << std::endl;
      // WriteLock w_lock(_reqRingMapLock);
      auto threadId = getThreadIdFromRequestId(requestId);
      WriteLock w_lock(*_reqRingMapLockList[threadId].get());
      if (_reqRingMap.count(requestId) != 0) {
        _reqRingMap.erase(requestId);
      }
      return;
    }
  }

  // NOTE: caller thread specific
  // TODO: Test
  uint64_t getLastRequestIdForThread() {
    // ReadLock r_lock(_reqRingMapLock);
    ReadLock r_lock(*_reqRingMapLockList[threadFsTid].get());
    auto endRequestIdItr = _reqRingMap.lower_bound((threadFsTid + 1) * REQUEST_ID_BASE);
    if ((--endRequestIdItr) == _reqRingMap.end()) {
      return 0;
    } else {
      return (--endRequestIdItr)->first; // key
    }
  }


  // NOTE: caller thread specific
  // TODO: Test
  std::map<uint64_t, std::vector<off_t>>::iterator getStartIteratorForThread() {
    // ReadLock r_lock(_reqRingMapLock);
    ReadLock r_lock(*_reqRingMapLockList[threadFsTid].get());
    return _reqRingMap.lower_bound(threadFsTid * REQUEST_ID_BASE);
  }

  // NOTE: caller thread specific
  // TODO: Test
  std::map<uint64_t, std::vector<off_t>>::iterator getEndIteratorForThread() {
    // ReadLock r_lock(_reqRingMapLock);
    ReadLock r_lock(*_reqRingMapLockList[threadFsTid].get());
    return _reqRingMap.lower_bound((threadFsTid + 1) * REQUEST_ID_BASE);
  }

  // Note: caller thread specific
  // Note: This is expensive
  // TODO: Improve on this
  std::map<uint64_t, std::vector<off_t>> getReqRingMap() {
    // ReadLock r_lock(_reqRingMapLock);
    ReadLock r_lock(*_reqRingMapLockList[threadFsTid].get());
    auto startRequestIdItr = getStartIteratorForThread();
    auto endRequestIdItr = getEndIteratorForThread();

    std::map<uint64_t, std::vector<off_t>> res;
    for (auto itr = startRequestIdItr; itr != endRequestIdItr; itr++) {
      res[itr->first] = itr->second;
    }
    // return std::copy(startRequestIdItr, endRequestIdItr, res);
    return res;
  }

  void addOffsetToRingMapForRequest(uint64_t requestId, off_t off) {
    // WriteLock w_lock(_reqRingMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    WriteLock w_lock(*_reqRingMapLockList[threadId].get());
    this->_reqRingMap[requestId].push_back(off);
  }

  // Data Map
  void removeRequestFromDataMap(uint64_t requestId) {
    auto threadId = getThreadIdFromRequestId(requestId);
    WriteLock w_lock(*_reqAllocDataMapLockList[threadId].get());
    // WriteLock w_lock(_reqAllocDataMapLock);
    if (this->_reqAllocatedDataMap.count(requestId) != 0) {
      void *buf = this->_reqAllocatedDataMap[requestId];
      this->_reqAllocatedDataMap.erase(requestId);
      fs_free(buf);
      buf = nullptr;
    }
    
  }

  void *getRequestBufPtrFromDataMap(uint64_t requestId) {
    // ReadLock r_lock(_reqAllocDataMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    ReadLock r_lock(*_reqAllocDataMapLockList[threadId].get());
    return this->_reqAllocatedDataMap[requestId];
  }

  void addBufPtrToDataMapForRequest(uint64_t requestId, void *buf) {
    // WriteLock w_lock(_reqAllocDataMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    WriteLock w_lock(*_reqAllocDataMapLockList[threadId].get());
    this->_reqAllocatedDataMap[requestId] = buf;
  }

  bool isRequestAnAllocatedType(uint64_t requestId) {
    // ReadLock r_lock(_reqAllocDataMapLock);
    auto threadId = getThreadIdFromRequestId(requestId);
    ReadLock r_lock(*_reqAllocDataMapLockList[threadId].get());
    return this->_reqAllocatedDataMap.count(requestId) != 0;
  }

  // periodic
  void detectServerUnavailLoop();
  std::thread detectServerAliveThread;

  int getPendingRequestPercentage() {
    // ReadLock r_lock(_reqRingMapLock);
    // int size = _reqRingMapLockList.size();
    // for (int i = 0; i < size; i++) {
    //   ReadLock r_lock(*_reqRingMapLockList[i].get());
    // }
    int ret = (_reqRingMap.size() * 100) / 64;
    // printf("[DEBUG] getPendingRequestPercentage: #reqs = %ld, percent = %d\n", _reqRingMap.size(), ret); fflush(stdout);
    return ret;
  }

  void appendNewLockToReqRingMapLockList() {
    _reqRingMapLockList.emplace_back(std::make_unique<std::shared_mutex>());
    _reqAllocDataMapLockList.emplace_back(std::make_unique<std::shared_mutex>());
  }

private:
  std::vector<bool> _notifyShmForThread;
  // requestId, [ring idx offset]
  std::map<uint64_t, std::vector<off_t>> _reqRingMap;
  // std::unordered_set<uint64_t> _leftoverRequestToBeCleaned; // create -> pending paradigm 
  std::unordered_map<uint64_t, void *> _reqAllocatedDataMap;
  int _backgroundThreadId = 10000;
  // Lock _reqRingMapLock; // TODO: Remove
  std::vector<std::unique_ptr<Lock>> _reqRingMapLockList;
  std::vector<std::unique_ptr<Lock>> _reqAllocDataMapLockList;
  // Lock _reqAllocDataMapLock; // TODO: Remove
};

class PendingQueueMgr {
 public:
  PendingQueueMgr(key_t shmKey);
  ~PendingQueueMgr();

  void init(key_t shmKey);
  off_t shmipc_mgr_alloc_slot_pending(struct shmipc_mgr *mgr);
  off_t enqueuePendingMsg(struct shmipc_msg* msgSrc);
  template <typename T>   
  void enqueuePendingXreq(T *opSrc, off_t idx);
  void enqueuePendingData(void *dataStr, off_t idx, 
    int size);
  void dequePendingMsg(uint64_t requestId);
  // void getPendingMsg();
  template <typename T>
  T* getPendingXreq(off_t idx);
  void* getPendingData(off_t idx);
  uint8_t getMessageStatus(uint64_t requestId);
  uint8_t getMessageType(off_t idx);

  struct shmipc_mgr *getShmManager();

  // TODO: Del later
  int getCountOfEmptySlots() {
    int count = 0;
    for (int i = 0; i < 64; i++) {
      auto msg = IDX_TO_MSG(queue_shmipc_mgr, i);
      if (msg->status == shmipc_STATUS_EMPTY) {
        count++;
      } else {
        // std::cout << "[DEBUG] off " << i << " is NOT EMPTY" << std::endl;
        // std::cout << "[DEBUG] has status =  " << unsigned(msg->status) << std::endl;
      }
    }

    return count;
  }
  
 private:
  struct shmipc_mgr *queue_shmipc_mgr = NULL;
};

struct FsLibServiceMng {
  pid_t fsServPid;
  std::unordered_map<int, FsService *> multiFsServMap;
  std::atomic_int multiFsServNum{0};
  FsService *primaryServ{nullptr};
  // must process in sorted order of id
  
  PendingQueueMgr *queueMgr{nullptr};
  RetryMgr *retryMgr{nullptr};
};

// NOTE: enable this flag will record the entry and exit timestamp
// of each fs api
// The point is to see how frequently the FS is stressed in realy apps
// in contrast to the case the in the microbenchmark, we keep issue fs request
// #define CFS_LIB_SAVE_API_TS

#ifdef CFS_LIB_SAVE_API_TS

enum class FsApiType {
  _DUMMY_FIRST,
  FS_STAT,
  FS_OPEN,
  FS_CLOSE,
  FS_UNLINK,
  FS_MKDIR,
  FS_OPENDIR,
  FS_READDIR,
  FS_CLOSEDIR,
  FS_RMDIR,
  FS_RENAME,
  FS_FSYNC,
  FS_READ,
  FS_PREAD,
  FS_WRITE,
  FS_PWRITE,
  _DUMMY_LAST,
};

using perApiTsVec = std::vector<std::array<uint64_t, 2>>;
const static int kNumApi = FsApiType::_DUMMY_LAST - FsApiType::_DUMMY_FIRST - 1;

#define FSAPI_TYPE_TO_IDX(tp)  \
  (static_cast<uint32_t>(tp) - \
   static_cast<uint32_t>(FsApiType::_DUMMY_FIRST) - 1)

#define FSAPI_TYPE_TO_STR(A)                              \
  {                                                       \
    (static_cast<uint32_t>(FsApiType::A) -                \
     static_cast<uint32_t>(FsApiType::_DUMMY_FIRST) - 1), \
        #A                                                \
  }

const static std::unordered_map<uint32_t, const char *> gFsApiTypeStrMap{
    FSAPI_TYPE_TO_STR(FS_STAT),    FSAPI_TYPE_TO_STR(FS_OPEN),
    FSAPI_TYPE_TO_STR(FS_CLOSE),   FSAPI_TYPE_TO_STR(FS_UNLINK),
    FSAPI_TYPE_TO_STR(FS_MKDIR),   FSAPI_TYPE_TO_STR(FS_OPENDIR),
    FSAPI_TYPE_TO_STR(FS_READDIR), FSAPI_TYPE_TO_STR(FS_CLOSEDIR),
    FSAPI_TYPE_TO_STR(FS_RMDIR),   FSAPI_TYPE_TO_STR(FS_RENAME),
    FSAPI_TYPE_TO_STR(FS_FSYNC),   FSAPI_TYPE_TO_STR(FS_READ),
    FSAPI_TYPE_TO_STR(FS_PREAD),   FSAPI_TYPE_TO_STR(FS_WRITE),
    FSAPI_TYPE_TO_STR(FS_PWRITE),
};

#define FSAPI_TS_START_ARR_POS (0)
#define FSAPI_TS_END_ARR_POS (1)

class FsApiTs {
 public:
  FsApiTs(int tid)
      : tid_(tid),
        apiTsList(kNumApi, perApiTsVec(kFsApiTsResizeStep)),
        apiTsIdxList(kNumApi, 0) {}

  void reportToStream(FILE *stream) {
    for (int typei = 0; typei < kNumApi; typei++) {
      auto &vec = apiTsList[typei];
      auto it = gFsApiTypeStrMap.find(typei);
      assert(it != gFsApiTypeStrMap.end());
      for (size_t j = 0; j < apiTsIdxList[typei]; j++) {
        fprintf(
            stream, "%s start:%lu end:%lu\n", it->second,
            PerfUtils::Cycles::toNanoseconds(vec[j][FSAPI_TS_START_ARR_POS]),
            PerfUtils::Cycles::toNanoseconds(vec[j][FSAPI_TS_END_ARR_POS]));
      }
    }
  }

  uint32_t addApiStart(FsApiType tp) {
    int typeidx = FSAPI_TYPE_TO_IDX(tp);
    auto &vec = apiTsList[typeidx];
    if (apiTsIdxList[typeidx] >= kFsApiTsResizeStep)
      vec.reserve(kFsApiTsResizeStep);
    (vec[apiTsIdxList[typeidx]])[FSAPI_TS_START_ARR_POS] = genTs();
    (vec[apiTsIdxList[typeidx]])[FSAPI_TS_END_ARR_POS] = 0;
    return apiTsIdxList[typeidx]++;
  }

  // @param inApiIdx is returned by addApiStart
  void addApiNormalDone(FsApiType tp, uint32_t inApiIdx) {
    int typeidx = FSAPI_TYPE_TO_IDX(tp);
    auto &vec = apiTsList[typeidx];
    if (inApiIdx > apiTsIdxList[typeidx] ||
        vec[inApiIdx][FSAPI_TS_END_ARR_POS] != 0) {
      throw std::runtime_error(
          "the index for this timestamp cannot match Or the finish has already "
          "been set");
    }
    vec[inApiIdx][FSAPI_TS_END_ARR_POS] = genTs();
  }

 private:
  int tid_;
  uint64_t genTs() { return PerfUtils::Cycles::rdtscp(); }
  const static uint32_t kFsApiTsResizeStep = 1000000;
  std::vector<perApiTsVec> apiTsList;
  std::vector<uint32_t> apiTsIdxList;
};

struct FsApiTsMng {
  // <tid, ts>
 public:
  FsApiTsMng() {}

  ~FsApiTsMng() {
    for (auto ele : ioThreadsApiTs) delete ele.second;
  }

  FsApiTs *initForTid(int tid) {
    auto it = ioThreadsApiTs.find(tid);
    if (it != ioThreadsApiTs.end()) return (it->second);
    FsApiTs *curApiTs = new FsApiTs(tid);
    ioThreadsApiTs[tid] = curApiTs;
    return curApiTs;
  }

  void reportAllTs() {
    for (auto ele : ioThreadsApiTs) {
      fprintf(stdout, "FSAPI report --- tid:%d\n", ele.first);
      (ele.second)->reportToStream(stdout);
    }
  }

  // <tid, ts>
  std::unordered_map<int, FsApiTs *> ioThreadsApiTs;
};

#undef FSAPI_TYPE_TO_IDX
#undef FSAPI_TS_START_ARR_POS
#undef FSAPI_TS_END_ARR_POS

#endif  // CFS_LIB_SAVE_API_TS

struct OpenLeaseMapEntry {
  std::atomic_char32_t ref;
  std::condition_variable cv;
  std::shared_mutex lock;
  OpenLease *lease;

  OpenLeaseMapEntry(int base_fd, const std::string &path, uint64_t size)
      : ref(0), lease(new OpenLease(base_fd, path, size)) {}
  ~OpenLeaseMapEntry() { delete lease; }
};

OpenLeaseMapEntry *LeaseRef(const char *path);
OpenLeaseMapEntry *LeaseRef(int fd);
void LeaseUnref(bool del = false);

struct FsLibSharedContext {
  /// used as unique ID for this application process
  key_t key;
  // <fd, workerId>: store each id is handled by which worker.
  // NOTE: since fd is assigned by FSP, it is guaranteed that
  // within the App, all fd is incremental
  tbb::concurrent_unordered_map<int, int8_t> fdWidMap;
  tbb::concurrent_unordered_map<std::string, int8_t> pathWidMap;
  // <tid, pointer to memCache>
  // tid: thread id assigned by this library
  // REQUIRED: access should be guarded by tidIncrLock
  // int tidIncr;
  tbb::concurrent_unordered_map<int, FsLibMemMng *> tidMemBufMap;
  std::atomic_int tidIncr;
  tbb::concurrent_unordered_map<int, std::vector<struct BlockRecordOfFile *>>
      fhIdPageMap;

  // <fd, fileHandleId>
  // REQUIRED: access should be guarded by fdFileHandleMapLock
  absl::flat_hash_map<int, struct FileHandle *> fdFileHandleMap;
  std::unordered_map<std::string, struct FileHandle *> fnameFileHandleMap;
  tbb::reader_writer_lock fdFileHandleMapLock;
  absl::flat_hash_map<int, off_t> fdOffsetMap;

  // <fd, currentOffset>
  tbb::concurrent_unordered_map<int, off_t> fdCurOffMap;

  PageCacheHelper pageCacheHelper;

  // not related to data, keep track of the lease timer
  FsLibLeaseMng leaseMng_;

  std::shared_mutex openLeaseMapLock;
  std::unordered_map<std::string, OpenLeaseMapEntry *> pathOpenLeaseMap;
  std::unordered_map<int, OpenLeaseMapEntry *> fdOpenLeaseMap;

  tbb::concurrent_unordered_map<std::string, LDBLease *> pathLDBLeaseMap;
  tbb::concurrent_unordered_map<int, LDBLease *> fdLDBLeaseMap;

#ifdef CFS_LIB_SAVE_API_TS
  FsApiTsMng apiTsMng_;
#endif
};

// =======
// op Helper functions
// from posix api arguments to ops
struct readOp *fillReadOp(struct clientOp *curCop, int fd, size_t count);
struct preadOp *fillPreadOp(struct clientOp *curCop, int fd, size_t count,
                            off_t offset);
struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count);
struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count);
struct pwriteOp *fillPWriteOp(struct clientOp *curCop, int fd, size_t count,
                              off_t offset);
struct openOp *fillOpenOp(struct clientOp *curCop, const char *path, int flags,
                          mode_t mode);
struct closeOp *fillCloseOp(struct clientOp *curCop, int fd);
struct statOp *fillStatOp(struct clientOp *curCop, const char *path,
                          struct stat *statbuf);
struct mkdirOp *fillMkdirOp(struct clientOp *curCop, const char *pathname,
                            mode_t mode);
struct fstatOp *fillFstatOp(struct clientOp *curCop, int fd,
                            struct stat *statbuf);
struct fsyncOp *fillFsyncOp(struct clientOp *curCop, int fd, bool isDataSync);

struct unlinkOp *fillUnlinkOp(struct clientOp *curCop, const char *pathname);

// Setup per thread mem buffer
// Check if the thread has already setup its mem buffer, if not set it up.
FsLibMemMng *check_app_thread_mem_buf_ready(bool isRetry, int fsTid = threadFsTid);
// =======

int fs_stat_internal_common(const char *pathname, struct stat *statbuf, uint64_t requestId);
int fs_fstat_internal_common(int fd, struct stat *statbuf, uint64_t requestId);
int fs_open_internal_common(const char *path, int flags, mode_t mode, uint64_t requestId);
int fs_close_internal_common(int fd, uint64_t requestId);
int fs_unlink_internal_common(const char *pathname, uint64_t requestId);
int fs_mkdir_internal_common(const char *pathname, mode_t mode, uint64_t requestId, bool isRetry);
CFS_DIR *fs_opendir_internal_common(const char *name, uint64_t requestId, bool isRetry);
int fs_fsync_internal_common(int fd, uint64_t requestId);
int fs_fdatasync_internal_common(int fd, uint64_t requestId);
ssize_t fs_read_internal_common(int fd, void *buf, size_t count, uint64_t requestId);
ssize_t fs_pread_internal_common(int fd, void *buf, size_t count, off_t offset, uint64_t requestId);
ssize_t fs_pwrite_internal_common(int fd, const void *buf, size_t count, off_t offset, uint64_t requestId);
ssize_t fs_write_internal_common(int fd, const void *buf, size_t count,  uint64_t requestId);
ssize_t fs_allocated_read_internal_common(int fd, void *buf, size_t count, 
  uint64_t requestId, bool isRetry);
  ssize_t fs_allocated_pread_internal_common(int fd, void *buf, size_t count, 
  off_t offset, uint64_t requestId, bool isRetry);
  ssize_t fs_allocated_write_internal_common(int fd, void *buf, size_t count, 
  uint64_t requestId, bool isRetry);
  ssize_t fs_allocated_pwrite_internal_common(int fd, void *buf, ssize_t count, 
  off_t offset, uint64_t requestId, bool isRetry);
// =======

void clean_up_notify_msg_from_server(bool shouldSleep = true);
void clean_up_notify_internal(shmipc_mgr *mgr, off_t ringIdx);

#endif  // CFS_FSLIBAPP_H

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <atomic>
#include <iostream>

#ifdef _CFS_LIB_PRINT_REQ_
#include <sys/syscall.h>
#include <sys/types.h>
#endif

#ifdef FS_LIB_SPPG
#include <memory>
#include <mutex>

#include "spdk/env.h"
#endif

#include <sys/stat.h>
#include <unistd.h>
#include <util.h>

#include <cassert>
#include <cstring>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "FsLibApp.h"
#include "FsMsg.h"
#include "FsProc_FsInternal.h"
#include "fsapi.h"
#include "param.h"
#include "shmipc/shmipc.h"
#include "stats/stats.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/reader_writer_lock.h"
#include "tbb/scalable_allocator.h"


// Function prototypes here
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
ssize_t fs_allocated_pread_retry(int fd, void *buf, size_t count, off_t offset, uint64_t requestId, void **bufPtr);

// if enabled, will print each api invocation
// #define _CFS_LIB_PRINT_REQ_
#define LDB_PRINT_CALL

/* #region requestId */
// TODO: Lock over gRequestId
uint64_t gRequestId = 0;

// TODO: Lock
uint64_t inline getNewRequestId() {
  return gRequestId++;
}

/* #endregion requestId end */

/* #region print api */

// TODO: Add retry module here
void print_server_unavailable(const char *func_name) {
  fprintf(stderr, "[WARN] Server is unavailable for function %s. Retrying all pending operations.\n", func_name);
}

void print_admin_inode_reassignment(int type, uint32_t inode, int curOwner,
                                int newOwner) {
  fprintf(stderr, "-- fs_admin_inode_reassignment(%d, %d, %d, %d) --\n", type, inode,
          curOwner, newOwner);                         
}

void print_open(const char *path, int flags, mode_t mode, bool ldb = false, uint64_t requestId = 0) {
  fprintf(stderr, "-- Req #%ld: open%s(%s, %d, %d) from tid %d --\n", requestId, ldb ? "_ldb" : "", path,
          flags, mode, threadFsTid);
}

void print_close(int fd, bool ldb = false, uint64_t requestId = 0) {
  fprintf(stderr, "-- Req#%ld: close%s(%d) from tid %d --\n", requestId, ldb ? "_ldb" : "", fd,
          threadFsTid);
}

void print_pread(int fd, void *buf, size_t count, off_t offset, uint64_t requestId = 0,
                 bool ldb = false) {
  fprintf(stderr, "-- Req #%ld: pread%s(%d, %p, %lu, %ld) from tid %d --\n", requestId,
        ldb ? "_ldb" : "", fd, buf, count, offset, threadFsTid);
}

void print_read(int fd, void *buf, size_t count, uint64_t requestId = 0, bool ldb = false) {
  fprintf(stderr, "-- Req #%ld: read%s(%d, %p, %lu) from tid %d --\n", requestId,
        ldb ? "_ldb" : "", fd, buf, count, threadFsTid);
}

void print_write(int fd, const void *buf, size_t count, uint64_t requestId = 0, 
  bool ldb = false) {
  fprintf(stderr, "-- Req #%ld: write%s(%d, %p, %lu) from tid %d --\n", requestId, ldb ? "_ldb" : "", fd,
          buf, count, threadFsTid);
}

void print_pwrite(int fd, const void *buf, size_t count, off_t offset, uint64_t requestId = 0) {
  fprintf(stderr, "-- Req #%ld: pwrite(%d, %p, %lu, %ld)\n --", requestId,
          fd, buf, count, offset);
}

void print_mkdir(const char *pathname, mode_t mode, uint64_t requestId = 0) {
  fprintf(stderr, "-- Req #%ld: mkdir(%s, %dd)\n --", requestId,
          pathname, mode);
}

void print_fsync(int fd, bool ldb = false) {
  fprintf(stderr, "-- fsync%s(%d) from tid %d --\n", ldb ? "_ldb" : "", fd,
          threadFsTid);
}

void print_unlink(const char *path) {
  fprintf(stderr, "-- unlink(%s) from tid %d --\n", path, threadFsTid);
}

void print_rename(const char *oldpath, const char *newpath) {
  fprintf(stderr, "-- rename(%s, %s) from tid %d --\n", oldpath, newpath,
          threadFsTid);
}

void print_opendir(const char *path, uint64_t requestId) {
  fprintf(stderr, "-- Req #%ld: opendir(%s) --\n", requestId, path);
}

void print_stat(const char *path, uint64_t requestId) {
  fprintf(stderr, "-- Req #%ld: stat(%s) --\n", requestId, path);
}

void print_fstat(int fd, uint64_t requestId) {
  fprintf(stderr, "-- Req#%ld: fstat(%d) --\n", requestId, fd);
}

/* #endregion print api end */

/* #region FS global variables */
// Global variables
// The master's workerID
static int gPrimaryServWid = APP_CFS_COMMON_KNOWLEDGE_MASTER_WID;
FsLibSharedContext *gLibSharedContext = nullptr;

#ifdef UFS_SOCK_LISTEN
static key_t g_registered_shm_key_base = 0;
static int g_registered_max_num_worker = -1;
static int g_registered_shmkey_distance = -1;
std::once_flag g_register_flag;
#endif

// Used for multiple Fs processes
static FsLibServiceMng *gServMngPtr = nullptr;
std::atomic_bool gCleanedUpDone;
std::atomic_flag gMultiFsServLock = ATOMIC_FLAG_INIT;

// Thread local variables
// if threadFsTid is 0, it means fsLib has not setup its FsLibMemMng
thread_local int threadFsTid = 0;

#ifdef CFS_LIB_SAVE_API_TS
thread_local FsApiTs *tFsApiTs;
#endif

#ifdef FS_LIB_SPPG
constexpr uint32_t kLocalPinnedMemSize = ((uint32_t)1024) * 1024 * 16;  // 16M

struct SpdkEnvWrapper {
  struct spdk_env_opts opts;
};

std::shared_ptr<SpdkEnvWrapper> gSpdkEnvPtr;
std::once_flag gSpdkEnvFlag;

struct FsLibPinnedMemMng {
  void *memPtr{nullptr};
};
thread_local std::unique_ptr<FsLibPinnedMemMng> tlPinnedMemPtr;
#endif

/* #endregion */

//
// helper functions to dump rbtree to stdout
//
constexpr static int kDumpRbtreeIndent = 4;
void dumpRbtree(rbtree_node n, int indent) {
  int i;
  if (n == nullptr) {
    fprintf(stdout, "<empty tree>");
    return;
  }
  if (n->right != nullptr) {
    dumpRbtree(n->right, indent + kDumpRbtreeIndent);
  }
  for (i = 0; i < indent; i++) {
    fprintf(stdout, " ");
  }
  if (n->color == BLACK) {
    fprintf(stdout, "%lu\n", (unsigned long)n->key);
  } else {
    fprintf(stdout, "<%lu>\n", (unsigned long)n->key);
  }
  if (n->left != NULL) {
    dumpRbtree(n->left, indent + kDumpRbtreeIndent);
  }
}

static inline off_t shmipc_mgr_alloc_slot_dbg(struct shmipc_mgr *mgr);

// REQUIRED: this must be called after caller calls fs_malloc()
// within it, \_setup_app_thread_mem_buf() will init threadFsTid
template <typename T>
static void EmbedThreadIdToAsOpRet(T &ret_field) {
#if 0
  if (threadFsTid <= 0 || threadFsTid >= 1000) {
    fprintf(stderr, "tid:%d\n", threadFsTid);
  }
#endif
  assert((threadFsTid != 0 && threadFsTid < 1000));
  ret_field = static_cast<T>(1);
}

static void dumpAllocatedOpCommon(allocatedOpCommonPacked *alOp) {
  if (alOp == nullptr) {
    return;
  }
  fprintf(stdout, "allocatedOp - shmId:%u dataPtrId:%u seqNo:%lu\n",
          alOp->shmid, alOp->dataPtrId, alOp->perAppSeqNo);
}

/* #region Notification handling start */
void inline handleSingleOpNotification(uint64_t reqId) {
  gServMngPtr->queueMgr->dequePendingMsg(reqId);
  gServMngPtr->reqRingMap.erase(reqId);
}

void handleCompositeOpNotification(uint64_t individualId) {
  // std::cout << "Inside " << __func__ << std::endl;
  assert(gServMngPtr->childToParentCompositeIdMap.count(individualId) != 0);
  uint64_t compositeId = gServMngPtr->childToParentCompositeIdMap[individualId];

  gServMngPtr->compositeRequestIdMap[compositeId].second.erase(individualId);
  gServMngPtr->childToParentCompositeIdMap.erase(individualId);

  if (gServMngPtr->compositeRequestIdMap[compositeId].first == true &&
    gServMngPtr->compositeRequestIdMap[compositeId].second.size() == 0) {
      gServMngPtr->compositeRequestIdMap.erase(compositeId);
      handleSingleOpNotification(compositeId);
   }

  // std::cout << "Exiting " << __func__ << std::endl;
}

void FsService::notificationListenerLoop() {
  while(true) {
    uint8_t count = 0;
    shmipc_msg msg;
    off_t ringIdx = 0;
    assert(shmipc_mgr != nullptr);
    // go over entire ring once
    do {
      memset(&msg, 0, sizeof(struct shmipc_msg));
      auto ret = shmipc_mgr_poll_notify_msg(shmipc_mgr, ringIdx, &msg);
      if (ret != -1) {                        
        handleServerNotification(msg.retval);
        shmipc_mgr_dealloc_slot(shmipc_mgr, ringIdx);        
      }
      ringIdx++;
    } while (ringIdx < RING_SIZE);

    sleep(5); // TODO: configure this
 };
}

void FsService::handleServerNotification(int64_t requestId) {
  if (gServMngPtr->childToParentCompositeIdMap.count(requestId) != 0) {
    handleCompositeOpNotification(requestId);
  } else {
    handleSingleOpNotification(requestId);
  }
}

void FsService::cleanupNotificationListener() {
  notificationListener_->join();
}

/* #endregion Notification handling */

/* #region FS Service */
FsService::FsService(int wid, key_t key)
    : unusedSlotsLock(ATOMIC_FLAG_INIT),
      shmKey(key),
      wid(wid),
      channelPtr(nullptr) {
  for (auto i = 0; i < RING_SIZE; i++) {
    unusedRingSlots.push_back(i);
  }
  initService();
}

// Assume 0 means no shmKey available
FsService::FsService() : FsService(0, 0) {}

FsService::~FsService() { shmipc_mgr_destroy(shmipc_mgr); }

void FsService::initService() {
  // TODO: if shmkey is known, init memory. Otherwise, init with FsProc
  if (shmKey == 0) {
    std::cerr << "ERROR initService (shmKey==0) does not supported yet"
              << std::endl;
    exit(1);
  }

  char shmfile[128];
  snprintf(shmfile, 128, "/shmipc_mgr_%03d", (int)shmKey);

  // We create the memory on server side, so for client this is 0
  shmipc_mgr = shmipc_mgr_init(shmfile, RING_SIZE, 0);
  if (shmipc_mgr == NULL) {
    std::cerr << "ERROR initService failed to initialize shared memory."
              << " errno : " << strerror(errno) << std::endl;
    throw std::runtime_error("initservice failed to initialize shared memory");
  }

    std::cout << "INFO initService connected to contrl shm at " << shmfile
            << std::endl;

    notificationListener_ = new std::thread(&FsService::notificationListenerLoop, this);
}

int FsService::allocRingSlot() {
  // try grab the lock
  while (unusedSlotsLock.test_and_set(std::memory_order_acquire))
    // spinlock
    ;
  if (unusedRingSlots.empty()) {
    unusedSlotsLock.clear(std::memory_order_release);
    return -1;
  }
  int slotId = unusedRingSlots.front();
  unusedRingSlots.pop_front();
  // release the lock
  unusedSlotsLock.clear(std::memory_order_release);
  return slotId;
}

void FsService::releaseRingSlot(int slotId) {
  assert(slotId >= 0 && slotId < RING_SIZE);
  while (unusedSlotsLock.test_and_set(std::memory_order_acquire))
    ;
  unusedRingSlots.push_back(slotId);
  unusedSlotsLock.clear(std::memory_order_release);
}

void *FsService::getSlotDataBufPtr(int slotId, bool reset) {
  if (!(slotId >= 0 && slotId < RING_SIZE)) {
    fprintf(stdout, "getSlotDataBufPtr for slotID:%d is not valid\n", slotId);
    throw std::runtime_error("getSlotDataBufPtr is not valid");
  }
  auto ptr = channelPtr->slotDataPtr(slotId);
  if (reset) memset(ptr, 0, RING_DATA_ITEM_SIZE);
  return ptr;
}

struct clientOp *FsService::getSlotOpPtr(int slotId) {
  return channelPtr->slotOpPtr(slotId);
}

// Assume the slot HAS BEEN filled with valid CfsOp before calling this function
// try submit until success
// @param slotId allocated by ->allocRingSlot
// @return
int FsService::submitReq(int slotId) {
  int ret;
  while ((ret = channelPtr->submitSlot(slotId)) != 1) {
    if (ret < 0) {
      std::cerr << "ERROR FsService::submitReq" << std::endl;
      break;
    }
  }
  return ret;
}
/* #endregion FS Service */

// Note: function is duplicated
int is_server_up(pid_t pid) {
  // no such process
  if (kill(pid, 0) != 0 && errno == ESRCH) {
    return 0;
  }

  return 1;
}

int get_server_pid() {
  // server pid will be placed in master shm
  auto primaryServer = gServMngPtr->primaryServ;
  shmipc_msg msg;
  off_t ringIdx = 0;
  assert(primaryServer->shmipc_mgr != nullptr);

  memset(&msg, 0, sizeof(struct shmipc_msg));
  auto ret = shmipc_mgr_poll_pid_msg(primaryServer->shmipc_mgr, 0 /*ringIdx*/, &msg);
  if (ret != -1 && is_server_up(msg.retval)) {                        
    gServMngPtr->fsServPid = msg.retval;
    // shmipc_mgr_dealloc_slot(primaryServer->shmipc_mgr, ringIdx); // don't dealloc
    return 0; // success 
  }

  return -1; // failure 
}

/* #region PendingQueueMgr start */
PendingQueueMgr::PendingQueueMgr(key_t shmKey) {
  init(shmKey);
}

PendingQueueMgr::~PendingQueueMgr() {
  shmipc_mgr_destroy(queue_shmipc_mgr);
}

void PendingQueueMgr::init(key_t shmKey) {
  // TODO: if shmkey is known, init memory. Otherwise, init with FsProc
  if (shmKey == 0) {
    std::cerr << "ERROR initService (shmKey==0) does not supported yet"
              << std::endl;
    exit(1);
  }

  char shmfile[128];
  snprintf(shmfile, 128, "/shmipc_mgr_%03d", (int)shmKey);

  // We create the memory on server side, so for client this is 0
  queue_shmipc_mgr = shmipc_mgr_init(shmfile, RING_SIZE, 1);
  if (queue_shmipc_mgr == NULL) {
    std::cerr << "ERROR PendingQueueMgr::init failed to initialize shared memory."
              << " errno : " << strerror(errno) << std::endl;
    throw std::runtime_error("PendingQueueMgr::init failed to initialize shared memory");
  }

  std::cout << "INFO PendingQueueMgr connected to contrl shm at " << shmfile
          << std::endl;
}

struct shmipc_mgr *PendingQueueMgr::getShmManager() {
  return this->queue_shmipc_mgr;
}

off_t PendingQueueMgr::enqueuePendingMsg(struct shmipc_msg* msgSrc) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);

  struct shmipc_msg queueMsg;
  memcpy(&queueMsg, msgSrc, sizeof(*msgSrc));

  off_t ringIdx = shmipc_mgr_alloc_slot_dbg(shmMgr);
  
  shmipc_mgr_put_msg_nowait(shmMgr, ringIdx, &queueMsg, FS_PENDING_STATUS);

  return ringIdx;
}

template <typename T>   
void PendingQueueMgr::enqueuePendingXreq(T *opSrc, off_t idx) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);
  
  T *opDest = (T *)IDX_TO_XREQ(shmMgr, idx);
  memcpy(opDest, opSrc, sizeof(*opSrc));
}

void PendingQueueMgr::enqueuePendingData(void *dataStr, off_t idx, 
  int size) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);

  void *dataDest = (void *)IDX_TO_DATA(shmMgr, idx);
  memcpy(dataDest, (char *)dataStr, size);
}

void PendingQueueMgr::dequePendingMsg(uint64_t requestId) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);
  
  for (auto ringIdx: gServMngPtr->reqRingMap[requestId]) {
    shmipc_mgr_dealloc_slot(shmMgr, ringIdx);
  }
}

template <typename T>
T* PendingQueueMgr::getPendingXreq(off_t idx) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);
  return (T *)IDX_TO_XREQ(shmMgr, idx);
}

void* PendingQueueMgr::getPendingData(off_t idx) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);
  return IDX_TO_DATA(shmMgr, idx);
}

uint8_t PendingQueueMgr::getMessageStatus(uint64_t requestId) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);

  if (gServMngPtr->reqRingMap.count(requestId) == 0 || 
   gServMngPtr->reqRingMap[requestId].size() == 0) {
    throw std::runtime_error("Invalid request id");
  }

  off_t ringIdx = gServMngPtr->reqRingMap[requestId][0];

  return (IDX_TO_MSG(shmMgr, ringIdx))->status;
}

uint8_t PendingQueueMgr::getMessageType(off_t idx) {
  auto shmMgr = gServMngPtr->queueMgr->getShmManager();
  assert(shmMgr != nullptr);

  return (IDX_TO_MSG(shmMgr, idx))->type;
}
/* #endregion PendingQueueMgr */

/* #region CommuChannelAppSide start */
CommuChannelAppSide::CommuChannelAppSide(pid_t id, key_t shmKey)
    : CommuChannel(id, shmKey) {
  int rt = this->initChannelMemLayout();
  if (rt < 0) throw "FsLibInit Error: Cannot init chammel memory";
}

CommuChannelAppSide::~CommuChannelAppSide() {
  cleanupSharedMemory();
  delete ringBufferPtr;
}

void *CommuChannelAppSide::attachSharedMemory() {
  assert(ringBufferPtr != nullptr);
  shmId = shmget(shmKey, totalSharedMemSize, S_IRUSR | S_IWUSR);
  if (shmId < 0) {
    return nullptr;
  }
  totalSharedMemPtr = (char *)shmat(shmId, NULL, 0);
  return (void *)totalSharedMemPtr;
}

void CommuChannelAppSide::cleanupSharedMemory() {
  int rc = shmdt(totalSharedMemPtr);
  if (rc == -1) {
    std::cerr << "ERROR cannot detach memory" << std::endl;
    exit(1);
  }
}

int CommuChannelAppSide::submitSlot(int sid) {
  int ret = ringBufferPtr->enqueue(sid);
  return ret;
}
/* #endregion CommuChannelAppSide end */

/* #region fs helper functions */
static inline void adjustPath(const char *path, char *opPath) {
  std::string s(path);
  strncpy(opPath, s.c_str(), MULTI_DIRSIZE);
}

static inline void fillRwOpCommon(struct rwOpCommon *opc, int fd,
                                  size_t count) {
  memset(opc, 0, sizeof(struct rwOpCommon));
  opc->fd = fd;
  opc->count = count;
  opc->flag = 0;
  if (FS_LIB_USE_APP_BUF) opc->flag |= _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_;
}

/* The following prepare_* functions are used to prepare a struct to be sent
 * over shmipc */
static inline void prepare_rwOpCommon(struct rwOpCommonPacked *op, int fd,
                                      size_t count, uint64_t requestId = 0) {
  op->fd = fd;
  op->count = count;
  op->flag = 0;
  op->requestId = requestId;
  EmbedThreadIdToAsOpRet(op->ret);
  if (FS_LIB_USE_APP_BUF) op->flag |= _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_;
}

struct allocatedReadOp *fillAllocedReadOp(struct clientOp *curCop, int fd,
                                          size_t count) {
  curCop->opCode = CFS_OP_ALLOCED_READ;
  curCop->opStatus = OP_NEW;
  struct allocatedReadOp *arop = &((curCop->op).allocread);
  fillRwOpCommon(&arop->rwOp, fd, count);
  return arop;
}

static inline void prepare_allocatedReadOp(struct shmipc_msg *msg,
                                           struct allocatedReadOpPacked *op,
                                           int fd, size_t count) {
  msg->type = CFS_OP_ALLOCED_READ;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct allocatedPreadOp *fillAllocedPreadOp(struct clientOp *curCop, int fd,
                                            size_t count, off_t offset) {
  curCop->opCode = CFS_OP_ALLOCED_PREAD;
  curCop->opStatus = OP_NEW;
  struct allocatedPreadOp *aprop = &((curCop->op).allocpread);
  fillRwOpCommon(&aprop->rwOp, fd, count);
  aprop->offset = offset;
  return aprop;
}

static inline void prepare_allocatedPreadOp(struct shmipc_msg *msg,
                                            struct allocatedPreadOpPacked *op,
                                            int fd, size_t count,
                                            off_t offset, uint64_t requestId = 0) {
  msg->type = CFS_OP_ALLOCED_PREAD;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count, requestId);
}

struct allocatedWriteOp *fillAllocedWriteOp(struct clientOp *curCop, int fd,
                                            size_t count) {
  curCop->opCode = CFS_OP_ALLOCED_WRITE;
  curCop->opStatus = OP_NEW;
  struct allocatedWriteOp *awop = &((curCop->op).allocwrite);
  fillRwOpCommon(&awop->rwOp, fd, count);
  return awop;
}

static inline void prepare_allocatedWriteOp(struct shmipc_msg *msg,
                                            struct allocatedWriteOpPacked *op,
                                            int fd, size_t count) {
  msg->type = CFS_OP_ALLOCED_WRITE;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct allocatedPwriteOp *fillAllocedPwriteOp(struct clientOp *curCop, int fd,
                                              size_t count, off_t offset) {
  curCop->opCode = CFS_OP_ALLOCED_PWRITE;
  curCop->opStatus = OP_NEW;
  struct allocatedPwriteOp *apwrop = &((curCop->op).allocpwrite);
  apwrop->offset = offset;
  fillRwOpCommon(&apwrop->rwOp, fd, count);
  return apwrop;
}

static inline void prepare_allocatedPwriteOp(struct shmipc_msg *msg,
                                             struct allocatedPwriteOpPacked *op,
                                             int fd, size_t count,
                                             off_t offset, uint64_t requestId) {
  msg->type = CFS_OP_ALLOCED_PWRITE;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count, requestId);
}

struct readOp *fillReadOp(struct clientOp *curCop, int fd, size_t count) {
  curCop->opCode = CFS_OP_READ;
  curCop->opStatus = OP_NEW;
  struct readOp *rop = &((curCop->op).read);
  fillRwOpCommon(&rop->rwOp, fd, count);
  return rop;
}

static inline void prepare_readOp(struct shmipc_msg *msg,
                                  struct readOpPacked *op, int fd,
                                  size_t count) {
  msg->type = CFS_OP_READ;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct preadOp *fillPreadOp(struct clientOp *curCop, int fd, size_t count,
                            off_t offset) {
  curCop->opCode = CFS_OP_PREAD;
  curCop->opStatus = OP_NEW;
  struct preadOp *prop = &((curCop->op).pread);
  prop->offset = offset;
  fillRwOpCommon(&prop->rwOp, fd, count);
  return prop;
}

static inline void prepare_preadOp(struct shmipc_msg *msg,
                                   struct preadOpPacked *op, int fd,
                                   size_t count, off_t offset) {
  msg->type = CFS_OP_PREAD;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

static inline void prepare_preadOpForUC(struct shmipc_msg *msg,
                                        struct preadOpPacked *op, int fd,
                                        size_t count, off_t offset) {
  msg->type = CFS_OP_PREAD;
  op->offset = offset;
  op->rwOp.realCount = 0;
  op->rwOp.realOffset = 0;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count) {
  curCop->opCode = CFS_OP_WRITE;
  curCop->opStatus = OP_NEW;
  struct writeOp *wop = &((curCop->op).write);
  fillRwOpCommon(&wop->rwOp, fd, count);
  return wop;
}

static inline void prepare_writeOp(struct shmipc_msg *msg,
                                   struct writeOpPacked *op, int fd,
                                   size_t count) {
  msg->type = CFS_OP_WRITE;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct pwriteOp *fillPWriteOp(struct clientOp *curCop, int fd, size_t count,
                              off_t offset) {
  curCop->opCode = CFS_OP_PWRITE;
  curCop->opStatus = OP_NEW;
  struct pwriteOp *wop = &((curCop->op).pwrite);
  wop->offset = offset;
  fillRwOpCommon(&wop->rwOp, fd, count);
  return wop;
}

static inline void prepare_pwriteOp(struct shmipc_msg *msg,
                                    struct pwriteOpPacked *op, int fd,
                                    size_t count, off_t offset, 
                                    uint64_t requestId) {
  msg->type = CFS_OP_PWRITE;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count, requestId);
}

static inline void prepare_lseekOp(struct shmipc_msg *msg, struct lseekOp *op,
                                   int fd, long int offset, int whence, off_t file_offset) {
  msg->type = CFS_OP_LSEEK;
  op->fd = fd;
  op->offset = offset;
  op->whence = whence;
  op->current_file_offset = file_offset;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct openOp *fillOpenOp(struct clientOp *curCop, const char *path, int flags,
                          mode_t mode) {
  curCop->opCode = CFS_OP_OPEN;
  curCop->opStatus = OP_NEW;
  struct openOp *oop = &((curCop->op).open);
  adjustPath(path, &(oop->path)[0]);
  oop->flags = flags;
  oop->mode = mode;
  return oop;
}

/* The remainder of prepare_* functions do not accept *Packed structs as we
 * aren't optimizing them for now.
 * TODO: find packed representations for the following structs as well */
static inline void prepare_openOp(struct shmipc_msg *msg, struct openOp *op,
                                  const char *path, int flags, mode_t mode,
                                  uint64_t *size = nullptr, uint64_t requestId = 0) {
  op->requestId = requestId;
  msg->type = CFS_OP_OPEN;
  op->flags = flags;
  op->mode = mode;
  op->lease = size != nullptr;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->path[0]));
}

struct closeOp *fillCloseOp(struct clientOp *curCop, int fd) {
  curCop->opCode = CFS_OP_CLOSE;
  curCop->opStatus = OP_NEW;
  struct closeOp *cloOp = &((curCop->op).close);
  cloOp->fd = fd;
  return cloOp;
}

static inline void prepare_closeOp(struct shmipc_msg *msg, struct closeOp *op,
                                   int fd, uint64_t requestId) {
  msg->type = CFS_OP_CLOSE;
  op->fd = fd;
  op->requestId = requestId;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct statOp *fillStatOp(struct clientOp *curCop, const char *path,
                          struct stat *statbuf) {
  curCop->opCode = CFS_OP_STAT;
  curCop->opStatus = OP_NEW;
  struct statOp *statop = &((curCop->op).stat);
  adjustPath(path, &(statop->path)[0]);
  memset(&(statop->statbuf), 0, sizeof(struct stat));
#ifdef _CFS_LIB_DEBUG_
  // fprintf(stderr, "fillStatOp - path:%s\n", statop->path);
#endif
  return statop;
}

static inline void prepare_statOp(struct shmipc_msg *msg, struct statOp *op,
                                  const char *path, uint64_t requestId) {
  msg->type = CFS_OP_STAT;
  op->requestId = requestId;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->path[0]));
}

struct mkdirOp *fillMkdirOp(struct clientOp *curCop, const char *pathname,
                            mode_t mode) {
  curCop->opCode = CFS_OP_MKDIR;
  curCop->opStatus = OP_NEW;
  struct mkdirOp *mkop = &((curCop->op).mkdir);
  mkop->mode = mode;
  adjustPath(pathname, &(mkop->pathname[0]));
  return mkop;
}

static inline void prepare_mkdirOp(struct shmipc_msg *msg, struct mkdirOp *op,
                                   const char *path, mode_t mode, bool isRetry, 
                                   uint64_t reqId) {
  msg->type = CFS_OP_MKDIR;
  op->mode = mode;
  op->requestId = reqId;
  op->isRetry = isRetry;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->pathname[0]));
}

struct fstatOp *fillFstatOp(struct clientOp *curCop, int fd,
                            struct stat *statbuf) {
  curCop->opCode = CFS_OP_FSTAT;
  curCop->opStatus = OP_NEW;
  struct fstatOp *fstatop = &((curCop->op).fstat);
  fstatop->fd = fd;
  return fstatop;
}

static inline void prepare_fstatOp(struct shmipc_msg *msg, struct fstatOp *op,
                                   int fd, uint64_t requestId) {
  msg->type = CFS_OP_FSTAT;
  op->fd = fd;
  op->requestId = requestId;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct renameOp *fillRenameOp(struct clientOp *curCop, const char *oldpath,
                              const char *newpath) {
  curCop->opCode = CFS_OP_RENAME;
  curCop->opStatus = OP_NEW;
  struct renameOp *rnmop = &((curCop->op).rename);
  adjustPath(oldpath, &(rnmop->oldpath[0]));
  adjustPath(newpath, &(rnmop->newpath[0]));
  return rnmop;
}

static inline void prepare_renameOp(struct shmipc_msg *msg, struct renameOp *op,
                                    const char *oldpath, const char *newpath) {
  msg->type = CFS_OP_RENAME;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(oldpath, &(op->oldpath[0]));
  adjustPath(newpath, &(op->newpath[0]));
}

struct rmdirOp *fillRmdirOp(struct clientOp *curCop, const char *pathname) {
  curCop->opCode = CFS_OP_RENAME;
  curCop->opStatus = OP_NEW;
  struct rmdirOp *rmdop = &((curCop->op).rmdir);
  EmbedThreadIdToAsOpRet(rmdop->ret);
  adjustPath(pathname, &(rmdop->pathname[0]));
  return rmdop;
}

struct fsyncOp *fillFsyncOp(struct clientOp *curCop, int fd, bool isDataSync) {
  curCop->opCode = CFS_OP_FSYNC;
  curCop->opStatus = OP_NEW;
  struct fsyncOp *fsyncop = &((curCop->op).fsync);
  fsyncop->fd = fd;
  if (isDataSync) {
    fsyncop->is_data_sync = 1;
  } else {
    fsyncop->is_data_sync = 0;
  }
  return fsyncop;
}

static inline void prepare_fsyncOp(struct shmipc_msg *msg, struct fsyncOp *op,
                                   int fd, bool isDataSync) {
  msg->type = CFS_OP_FSYNC;
  op->fd = fd;
  op->is_data_sync = (isDataSync) ? 1 : 0;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct unlinkOp *fillUnlinkOp(struct clientOp *curCop, const char *pathname) {
  curCop->opCode = CFS_OP_UNLINK;
  curCop->opStatus = OP_NEW;
  struct unlinkOp *unlkop = &((curCop->op).unlink);
  adjustPath(pathname, &(unlkop->path)[0]);
  return unlkop;
}

static inline void prepare_unlinkOp(struct shmipc_msg *msg, struct unlinkOp *op,
                                    const char *pathname, uint64_t reqId) {
  msg->type = CFS_OP_UNLINK;
  adjustPath(pathname, &(op->path[0]));
  op->requestId = reqId;
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_opendirOp(struct shmipc_msg *msg,
                                     struct opendirOp *op,
                                     const char *pathname, uint64_t requestId) {
  msg->type = CFS_OP_OPENDIR;
  op->requestId = requestId;
  adjustPath(pathname, &(op->name[0]));
  EmbedThreadIdToAsOpRet(op->numDentry);
}

static inline void prepare_rmdirOp(struct shmipc_msg *msg, struct rmdirOp *op,
                                   const char *pathname) {
  msg->type = CFS_OP_RMDIR;
  adjustPath(pathname, &(op->pathname[0]));
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_exitOp(struct shmipc_msg *msg, struct exitOp *op) {
  msg->type = CFS_OP_EXIT;
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_chkptOp(struct shmipc_msg *msg, struct chkptOp *op) {
  msg->type = CFS_OP_CHKPT;
}

static inline void prepare_migrateOp(struct shmipc_msg *msg,
                                     struct migrateOp *op, int fd) {
  msg->type = CFS_OP_MIGRATE;
  op->fd = fd;
}

static inline void prepare_dumpinodesOp(struct shmipc_msg *msg,
                                        struct dumpinodesOp *op) {
  msg->type = CFS_OP_DUMPINODES;
}

static inline void prepare_syncallOp(struct shmipc_msg *msg,
                                     struct syncallOp *synop) {
  msg->type = CFS_OP_SYNCALL;
  EmbedThreadIdToAsOpRet(synop->ret);
}

static inline void prepare_syncunlinkedOp(struct shmipc_msg *msg,
                                          struct syncunlinkedOp *synop) {
  msg->type = CFS_OP_SYNCUNLINKED;
  EmbedThreadIdToAsOpRet(synop->ret);
}

// Check the size of request's io is less than one slot's attached data.
// TODO (jingliu): Ideally, we can hide this kind of detail and do the
// multiplexing when the request is larger. Currently limit the io size for
// simplicity, App can still multiplex by themselves. In the end, we need to
// support very large request.
static inline bool fs_check_req_io_size(size_t count, const char *func_name) {
  if (count > RING_DATA_ITEM_SIZE) {
    fprintf(stderr, "ERROR %s io too large (count=%lu) not supported\n",
            func_name, count);
    return false;
  }
  return true;
}

static inline void prepare_pingOp(struct shmipc_msg *msg, struct pingOp *op) {
  msg->type = CFS_OP_PING;
}
/* #endregion fs helper functions */

// send an op without argument to given FsService
// TODO: Add to pending
template <typename NoArgOp>
static int send_noargop(FsService *fsServ, CfsOpCode opcode) {
  struct shmipc_msg msg;
  NoArgOp *op;
  int ret;

  off_t ring_idx;
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  op = reinterpret_cast<NoArgOp *> IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  // prep this msg with opcode only
  memset(&msg, 0, sizeof(msg));
  msg.type = opcode;

  // send msg
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    // ret = fs_retry_pending_ops();
    goto end;
  }

  // extract result
  ret = op->ret;
end:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

static inline off_t shmipc_mgr_alloc_slot_dbg(struct shmipc_mgr *mgr) {
#ifdef _SHMIPC_DBG_
  off_t ring_idx = shmipc_mgr_alloc_slot(mgr);
  fprintf(stdout, "tid:%d ring_idx:%zu\n", threadFsTid, ring_idx);
  return ring_idx;
#else
  return shmipc_mgr_alloc_slot(mgr);
#endif
}

/* #region global util functions */
// Global utility function for resolivng FsService given a file descriptor
inline FsService *getFsServiceForFD(int fd, int &wid) {
  auto &fdMap = gLibSharedContext->fdWidMap;
  auto search = fdMap.find(fd);
  if (search != fdMap.end()) {
    wid = search->second;
    auto service = gServMngPtr->multiFsServMap[wid];
    service->inUse = true;
    return service;
  }

  // TODO (ask jing) - do fd's also default to primary if they aren't in the
  // map?
  wid = gPrimaryServWid;
  // add new entry; TODO [BENITA] Needed due to client retries
  gLibSharedContext->fdWidMap[fd] = wid;
  return gServMngPtr->primaryServ;
}

inline FsService *getFsServiceForPath(const char *path, int &wid) {
  auto &pathMap = gLibSharedContext->pathWidMap;
  auto search = pathMap.find(path);
  if (search != pathMap.end()) {
    wid = search->second;
    auto service = gServMngPtr->multiFsServMap[wid];
    service->inUse = true;
    return service;
  }

  wid = gPrimaryServWid;
  return gServMngPtr->primaryServ;
}

// getWidFromReturnCode returns a wid >= 0 when rc indicates that an inode has
// migrated to a diffrent worker and needs to be retried. If wid < 0, it can be
// safely ignored.
inline int getWidFromReturnCode(int rc) {
  if (rc > FS_REQ_ERROR_INODE_REDIRECT) return -1;

  int newWid = FS_REQ_ERROR_INODE_REDIRECT - rc;
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "detected wid from return code (%d)\n", newWid);
#endif
  return newWid;
}

// FIXME: this is not thread-safe right now
// check the return value of one operation to see if we need to change wid
// If so, do the update according to the return value and op
// @retVal : do not want to check type inside, so let caller directly pass in
//   the return value. it should be mostly the *ret* field
// @return : if update, set to true
bool checkUpdateFdWid(int retVal, int fd) {
  // TODO : maintain an internal mapping of fd to path? We would want to
  // update the path mapping as well.
  int newWid = getWidFromReturnCode(retVal);
  if (newWid >= 0) {
    auto widIt = gLibSharedContext->fdWidMap.find(fd);
    if (widIt != gLibSharedContext->fdWidMap.end()) {
      widIt->second = newWid;
    } else {
      fprintf(stderr, "checkUpdateFdWid cannot find fd's worker\n");
      throw std::runtime_error("checkUpdateFdWid cannot find fd worker");
    }
    return true;
  }
  return false;
}

// NOTE: gLibSharedContent->pathWidMap is NOT supposed to have wid=0 as key
// Because the default wid is 0, if not found, then send to Primary worker
// NOTE: pathWidMap is not thread-safe for *erase*, let's see if this will
// result in something seriously bad (e.g., endless RETRY?)
void updatePathWidMap(int8_t newWid, const char *path) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stderr, "newWid is %d\n", (int)newWid);
#endif
  if (newWid == 0) {
    auto it = gLibSharedContext->pathWidMap.find(path);
    if (it != gLibSharedContext->pathWidMap.end()) {
      int cnt = gLibSharedContext->pathWidMap.unsafe_erase(path);
      if (cnt < 1) {
        // FIXME: what should we do here if it fail. not a big deal?
        fprintf(stderr, "pathWidMap erase path, cnt:%d\n", cnt);
      }
    }
  } else {
    gLibSharedContext->pathWidMap.emplace(path, newWid);
  }
}

// Spins for a little while if the return code indicates inode in transfer.
// Returns true/false to indicate whether it spun or not.
inline bool handle_inode_in_transfer(int rc) {
  if (rc == FS_REQ_ERROR_INODE_IN_TRANSFER) {
    uint64_t start_ts = tap_ustime();
    static const uint64_t k_interval = 1;
    while (tap_ustime() - start_ts < k_interval) continue;
    return true;
  }
  return false;
}

/* #endregion global util functions */

uint8_t fs_notify_server_new_shm(const char *shmFname,
                                 fslib_malloc_block_sz_t block_sz,
                                 fslib_malloc_block_sz_t block_cnt) {
  struct shmipc_msg msg;
  struct newShmAllocatedOp *aop;
  off_t ring_idx;
  uint8_t ret;

  memset(&msg, 0, sizeof(msg));
  msg.type = CFS_OP_NEW_SHM_ALLOCATED;

  ring_idx = shmipc_mgr_alloc_slot_dbg(gServMngPtr->primaryServ->shmipc_mgr);
  aop = (struct newShmAllocatedOp *)IDX_TO_XREQ(
      gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  nowarn_strncpy(aop->shmFname, shmFname, MULTI_DIRSIZE);
  aop->shmBlockSize = block_sz;
  aop->shmNumBlocks = block_cnt;

  // TODO: Not sure if exponential backoff is needed
  shmipc_mgr_put_msg(gServMngPtr->primaryServ->shmipc_mgr, ring_idx, &msg);

  ret = aop->shmid;
  shmipc_mgr_dealloc_slot(gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  return ret;
}

// NOTE: this function is only used for check_app_thread_mem_buf_read()
// Never use it directly in the implementation of File system APIs
// REQUIRED: write lock of gLibSharedContext->tidIncrLock needs to be held to
// enter this function
void _setup_app_thread_mem_buf() {
  assert(threadFsTid == 0);
#ifdef _CFS_LIB_PRINT_REQ_
  pid_t curPid = syscall(__NR_gettid);
  fprintf(stderr, "===> _setup_app_thread_mem_buf() called for pid:%d\n",
          curPid);
#endif
  dumpAllocatedOpCommon(nullptr);
  // int curThreadFsTid = gLibSharedContext->tidIncr++;
  int curThreadFsTid = 1; // TODO: Note this change
  threadFsTid = curThreadFsTid;
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs = gLibSharedContext->apiTsMng_.initForTid(threadFsTid);
  if (tFsApiTs == nullptr) throw std::runtime_error("error cannot get apiTs");
#endif
  auto curFsLibMemBuf = new FsLibMemMng(gLibSharedContext->key, threadFsTid);
  int rc = curFsLibMemBuf->init(true);
  if (rc < 0) {
    fprintf(stderr, "ERROR cannot init FsLibMemMng for tid:%d\n", threadFsTid);
  }

  int numShmFiles = curFsLibMemBuf->getNumShmFiles();
  char cur_shmFname[MULTI_DIRSIZE];
  fslib_malloc_block_sz_t cur_block_sz;
  fslib_malloc_block_sz_t cur_block_cnt;
  uint8_t cur_shmid;

  for (int idx = 0; idx < numShmFiles; idx++) {
    curFsLibMemBuf->getShmConfigForIdx(idx, cur_shmFname, cur_block_sz,
                                       cur_block_cnt, cur_shmid);
    
    cur_shmid =
        fs_notify_server_new_shm(cur_shmFname, cur_block_sz, cur_block_cnt);
    if (cur_shmid < 0) {
      fprintf(stderr, "ERROR cannot notify server of new shm for tid:%d\n",
              threadFsTid);
    }
    curFsLibMemBuf->setShmIDForIdx(idx, cur_shmid);
  }

 
  gLibSharedContext->tidMemBufMap.insert(
    std::make_pair(threadFsTid, curFsLibMemBuf));
  
}

FsLibMemMng *check_app_thread_mem_buf_ready(int fsTid) {
  // auto curLock = &(gLibSharedContext->tidIncrLock);
  FsLibMemMng *rt = nullptr;
  std::cout << "[DEBUG] threadFsTid = " << threadFsTid << std::endl;
  if (fsTid == 0) {
    _setup_app_thread_mem_buf();
    rt = gLibSharedContext->tidMemBufMap[threadFsTid];
#ifdef _CFS_LIB_DEBUG_
    fprintf(stderr, "check_app_thread_mem_buf_ready() threadFsTid == 0 -> %d\n",
            threadFsTid);
#endif
  } else {
    rt = gLibSharedContext->tidMemBufMap[fsTid];
  }

  assert(rt->isInitDone());
  return rt;
}

void fs_register_via_socket() {
  struct sockaddr_un addr;
  struct sockaddr_un server_addr;
  struct UfsRegisterOp rgst_op;
  struct UfsRegisterAckOp rgst_ack_op;
  memset(&rgst_op, 0, sizeof(rgst_op));
  memset(&rgst_ack_op, 0, sizeof(rgst_ack_op));
  pid_t my_pid = getpid();
  rgst_op.emu_pid = my_pid;

  constexpr int kPathLen = 64;
  char my_recv_sock_path[kPathLen];
  char server_sock_path[kPathLen];
  sprintf(my_recv_sock_path, "/ufs-app-%d", my_pid);
  sprintf(server_sock_path, "%s", CRED_UNIX_SOCKET_PATH);

  addr.sun_family = AF_UNIX;
  nowarn_strncpy(addr.sun_path, my_recv_sock_path, kPathLen);
  server_addr.sun_family = AF_UNIX;
  nowarn_strncpy(server_addr.sun_path, server_sock_path, kPathLen);

  int sfd = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (sfd == -1) {
    fprintf(stderr, "Error: socket failed [%s]\n", strerror(errno));
  }
  if (bind(sfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_un)) == -1) {
    printf("Error: bind failed [%s]\n", strerror(errno));
    close(sfd);
  }

  struct msghdr msgh;
  msgh.msg_name = (void *)&server_addr;
  msgh.msg_namelen = sizeof(server_addr);
  struct iovec iov;
  msgh.msg_iov = &iov;
  msgh.msg_iovlen = 1;
  iov.iov_base = &rgst_op;
  iov.iov_len = sizeof(rgst_op);
  msgh.msg_control = NULL;
  msgh.msg_controllen = 0;

  ssize_t ns = sendmsg(sfd, &msgh, 0);
  if (ns == -1) {
    fprintf(stderr, "Error: sendmsg return -1\n");
  }

  iov.iov_base = &rgst_ack_op;
  iov.iov_len = sizeof(rgst_ack_op);
  ssize_t nr = recvmsg(sfd, &msgh, 0);
  if (nr == -1) {
    fprintf(stderr, "Error: recv ack return -1\n");
  }
#ifdef UFS_SOCK_LISTEN
  g_registered_max_num_worker = rgst_ack_op.num_worker_max;
  g_registered_shm_key_base = rgst_ack_op.shm_key_base;
  g_registered_shmkey_distance = rgst_ack_op.worker_key_distance;
#endif
}

static void print_app_zc_mimic() {
#ifndef MIMIC_APP_ZC
  fprintf(stdout, "MIMIC_APP_ZC - OFF\n");
#else
  fprintf(stdout, "MIMIC_APP_ZC - ON\n");
#endif
}

/* #region fs util */
int fs_exit() {
  int ret = 0;

  for (auto iter : gServMngPtr->multiFsServMap) {
    auto service = iter.second;
    if (!(service->inUse)) continue;

    auto wid = iter.first;
    struct shmipc_msg msg;
    struct exitOp *eop;
    off_t ring_idx;
    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
    eop = (struct exitOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
    prepare_exitOp(&msg, eop);
    // fprintf(stdout, "fs_exit: for wid %d\n", wid);
    // TODO: Not sure if exponential backoff is needed here
    shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);
    ret = eop->ret;
    shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

    if (ret < 0) {
      fprintf(stderr, "fs_exit: failed for wid %d\n", wid);
      return ret;
    }
    // Commenting this out so the syscall_intercept has pid across cmd invocations
    // shmipc_mgr_client_reset(service->shmipc_mgr);
  }
#ifdef CFS_LIB_SAVE_API_TS
  gLibSharedContext->apiTsMng_.reportAllTs();
#endif
  pid_t my_pid = getpid();
  char my_recv_sock_path[128];
  sprintf(my_recv_sock_path, "/ufs-app-%d", my_pid);
  remove(my_recv_sock_path);
  return ret;
}

int fs_ping() {
  int ret = -1;

  for (auto iter : gServMngPtr->multiFsServMap) {
    std::cout << __func__ << "\t" << __LINE__ << std::endl; 
    auto service = iter.second;
    if (!(service->inUse)) continue;

    std::cout << __func__ << "\t" << __LINE__ << std::endl; 
    auto wid = iter.first;
    struct shmipc_msg msg;
    struct pingOp *eop;
    off_t ring_idx;
    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
    eop = (struct pingOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
    prepare_pingOp(&msg, eop);
    // Note: blocking call
    shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);
    ret = eop->ret;
    shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);
    if (ret < 0) {
      fprintf(stderr, "fs_ping: failed for wid %d\n", wid);
      return ret;
    }
  }
  return ret;
}

int fs_start_dump_load_stats() {
  auto fsServ = gServMngPtr->primaryServ;
  int ret = send_noargop<startDumpLoadOp>(fsServ, CFS_OP_STARTDUMPLOAD);
  return ret;
}

int fs_stop_dump_load_stats() {
  auto fsServ = gServMngPtr->primaryServ;
  int ret = send_noargop<stopDumpLoadOp>(fsServ, CFS_OP_STOPDUMPLOAD);
  return ret;
}

int fs_cleanup() {
  if (gCleanedUpDone) return 0;
  while (!gCleanedUpDone) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      for (auto ele : gServMngPtr->multiFsServMap) {
        // fprintf(stderr, "fs_cleanup: key:%d\n", ele.first);
        auto server = ele.second;
        server->cleanupNotificationListener();
        delete server;
        server = nullptr;
      }
      gServMngPtr->multiFsServMap.clear();
      gLibSharedContext = nullptr;
      gServMngPtr->primaryServ = nullptr;
      gMultiFsServLock.clear(std::memory_order_release);
    }

    gCleanedUpDone = true;
  }
  return 0;
}

int fs_checkpoint() {
  struct shmipc_msg msg;
  struct chkptOp *cop;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(gServMngPtr->primaryServ->shmipc_mgr);
  cop = (struct chkptOp *)IDX_TO_XREQ(gServMngPtr->primaryServ->shmipc_mgr,
                                      ring_idx);
  prepare_chkptOp(&msg, cop);
  // TODO: Not sure if exponential backoff is need here
  shmipc_mgr_put_msg(gServMngPtr->primaryServ->shmipc_mgr, ring_idx, &msg);

  ret = cop->ret;
  shmipc_mgr_dealloc_slot(gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_migrate(int fd, int *dstWid) {
  struct shmipc_msg msg;
  struct migrateOp *mop;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));

  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);

  ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
  mop = (struct migrateOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
  prepare_migrateOp(&msg, mop, fd);
  // TODO: Not sure if exponential backoff is need here
  shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);

  ret = mop->ret;
  shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

  checkUpdateFdWid(ret, fd);
  *dstWid = getWidFromReturnCode(ret);

  if ((ret == FS_REQ_ERROR_INODE_IN_TRANSFER) || (*dstWid >= 0)) {
    return 0;
  }
  return ret;
}

static inline void prepare_inodeReassignmentOp(struct shmipc_msg *msg,
                                               struct inodeReassignmentOp *op,
                                               int type, uint32_t inode,
                                               int curOwner, int newOwner) {
  msg->type = CFS_OP_INODE_REASSIGNMENT;
  op->type = type;
  op->inode = inode;
  op->curOwner = curOwner;
  op->newOwner = newOwner;
}

// admin command for testing, can be removed from header for user library.
// TODO: use an enum for type. However, I'd like one definition that can be
// shared by both fsapi and FsProc, which hasn't been done yet. For now, the
// values for type are
//
//      Messages must be sent to curOwner wid. i.e. if you send curOwner=2 to
//      wid 0, EACCES is returned. Returning EINVAL indicates the type is
//      invalid. Returning EPERM indicates not the current owner.
//
// (type=0) Check if curOwner is the owner of the inode. newOwner is ignored.
//      Returning 0 indicates success - it is the owner.
// (type=1) Move inode from curOwner to newOwner if curOwner is really the
// owner.
//      Returning 0 indicates successful migration.
int fs_admin_inode_reassignment(int type, uint32_t inode, int curOwner,
                                int newOwner) {
#ifdef LDB_PRINT_CALL
  print_admin_inode_reassignment(type, inode, curOwner, newOwner);
#endif
  
  auto search = gServMngPtr->multiFsServMap.find(curOwner);
  if (search == gServMngPtr->multiFsServMap.end()) {
    throw std::runtime_error("Wid not found in multiFsServMap");
  }

  auto service = search->second;
  struct shmipc_msg msg;
  struct inodeReassignmentOp *irop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
  irop = (decltype(irop))IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
  prepare_inodeReassignmentOp(&msg, irop, type, inode, curOwner, newOwner);

  // TODO: Not sure if exponential backoff is need here
  shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);

  ret = irop->ret;
  shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

  return ret;
}

static inline void prepare_threadReassignOp(struct shmipc_msg *msg,
                                            struct threadReassignOp *op,
                                            int tid, int src_wid, int dst_wid,
                                            int flag = FS_REASSIGN_ALL) {
  msg->type = CFS_OP_THREAD_REASSIGNMENT;
  op->tid = tid;
  op->src_wid = src_wid;
  op->dst_wid = dst_wid;
  op->ret = 0;
  op->flag = flag;
}

static inline bool CheckValidWid(int wid) {
  return (wid >= 0 && wid < NMAX_FSP_WORKER);
}

// testing and experimenting
// change the handling work of the entity: pid+tid from src_wid to dst_wid
int fs_admin_thread_reassign(int src_wid, int dst_wid, int flag) {
  if (src_wid == dst_wid ||
      (!CheckValidWid(src_wid) || !CheckValidWid(dst_wid))) {
    return -1;
  }
  auto serv_it = gServMngPtr->multiFsServMap.find(src_wid);
  if (serv_it == gServMngPtr->multiFsServMap.end()) {
    throw std::runtime_error("src_wid not existing src_wid=" +
                             std::to_string(src_wid));
  }
  struct shmipc_msg msg;
  struct threadReassignOp *irop;
  off_t ring_idx;
  int ret = 0;

  int cur_tid = threadFsTid;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(serv_it->second->shmipc_mgr);
  irop = (decltype(irop))IDX_TO_XREQ(serv_it->second->shmipc_mgr, ring_idx);

  prepare_threadReassignOp(&msg, irop, cur_tid, src_wid, dst_wid, flag);
  // TODO: Not sure if exponential backoff is need here
  shmipc_mgr_put_msg(serv_it->second->shmipc_mgr, ring_idx, &msg);

  ret = irop->ret;
  shmipc_mgr_dealloc_slot(serv_it->second->shmipc_mgr, ring_idx);
  if (irop->ret < 0) {
    return ret;
  }

  if (src_wid != gPrimaryServWid && flag != FS_REASSIGN_PAST) {
    // send to primary for updating future handling worker
    struct threadReassignOp *pri_irop;
    off_t pri_ring_idx;
    struct shmipc_msg pri_msg;
    auto pri_serv = gServMngPtr->primaryServ;

    pri_ring_idx = shmipc_mgr_alloc_slot_dbg(pri_serv->shmipc_mgr);
    pri_irop =
        (decltype(pri_irop))IDX_TO_XREQ(pri_serv->shmipc_mgr, pri_ring_idx);
    prepare_threadReassignOp(&pri_msg, pri_irop, cur_tid, src_wid, dst_wid);
    // TODO: Not sure if exponential backoff is need here
    shmipc_mgr_put_msg(pri_serv->shmipc_mgr, pri_ring_idx, &pri_msg);
    if (pri_irop->ret < 0) {
      fprintf(stderr, "Error from pri orig_ret:%d ret:%d\n", ret,
              pri_irop->ret);
      ret = pri_irop->ret;
    }
    shmipc_mgr_dealloc_slot(pri_serv->shmipc_mgr, pri_ring_idx);
  }

  return ret;
}

int fs_dumpinodes_internal(FsService *fsServ) {
  int ret = send_noargop<dumpinodesOp>(fsServ, CFS_OP_DUMPINODES);
  return ret;
}

// TODO: Lock
// NOTE: For testing purposes only
int fs_dump_pendingops() {
  for (auto &[key, ringList]: gServMngPtr->reqRingMap) {
    for (auto ringIdx: ringList) {
      std::cout << "Req: " << key << "\t"
      << "ringIdx: " << ringIdx << "\t" 
      << "request type: ";

      auto type = unsigned(gServMngPtr->queueMgr->getMessageType(ringIdx));
      switch(type) {
        case CFS_OP_PWRITE:
          std::cout << "pwrite" << std::endl;
          break;
        case CFS_OP_CREATE:
          std::cout << "create" << std::endl;
          break;
        case CFS_OP_PREAD:
          std::cout << "pread" << std::endl;
          break;
        case CFS_OP_UNLINK:
          std::cout << "unlink" << std::endl;
          break;
        case CFS_OP_MKDIR:
          std::cout << "mkdir" << std::endl;
          break;
        case CFS_OP_CP:
          std::cout << "cp" << std::endl;
          break;
        default:
          std::cout << type << " unknown" << std::endl;
          break;
      }
    }
  }

  return 0;
}

int fs_dumpinodes(int wid) {
  auto serv = gServMngPtr->multiFsServMap.find(wid);
  if (serv != gServMngPtr->multiFsServMap.end() && serv->second->inUse) {
    return fs_dumpinodes_internal(serv->second);
  } else {
    return -1;
  }
}

int check_primary_server_availability() {
  int count = 0;
  while (get_server_pid() != 0) {
    // std::cout << "#" << count++ << ": Trying to establish connection with server" << std::endl;
    // fflush(stdout);
    sleep(1);
  }

  return 0;
}

ssize_t handle_pwrite_retry(uint64_t reqId) {
  int count = 0;
  void *data = nullptr;
  int offset = 0;
  auto ringOffsetList = gServMngPtr->reqRingMap[reqId];
  int fd = 0;

  // get count
  for (auto ringIndex: ringOffsetList) {
    count += gServMngPtr->queueMgr->getPendingXreq<struct pwriteOpPacked>(ringIndex)->rwOp.count;
  }

  // get offset & inode
  fd = gServMngPtr->queueMgr->getPendingXreq<struct pwriteOpPacked>(ringOffsetList[0])->rwOp.fd;
  offset = gServMngPtr->queueMgr->getPendingXreq<struct pwriteOpPacked>(ringOffsetList[0])->offset;

  // get data
  data = (char *) calloc(count, sizeof(char));
  for (auto ringIndex: ringOffsetList) {
    memcpy(data, 
      gServMngPtr->queueMgr->getPendingData(ringIndex), 
      gServMngPtr->queueMgr->getPendingXreq<struct pwriteOpPacked>(ringIndex)->rwOp.count);
  }
  
  // retry
  auto ret = fs_pwrite_retry(fd, data, count, offset, reqId);

  // cleanup
  if (data != nullptr) {
    free(data);
  }

  return ret;
}

ssize_t handle_allocated_pwrite_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct allocatedPwriteOpPacked>(gServMngPtr->reqRingMap[reqId][0]);
  char *data = gServMngPtr->reqAllocatedDataMap[reqId];
  char *buf = (char *)fs_malloc(48 * 1024);
  strcpy(buf, data);
  auto ret = fs_allocated_pwrite_retry(op->rwOp.fd, buf, op->rwOp.count, op->offset, reqId);

  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

int handle_create_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct openOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_open_retry(op->path, op->flags, op->mode, reqId);
  return ret;
}

ssize_t handle_pread_retry(uint64_t reqId, void* buf) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct preadOpPacked>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_pread_retry(op->rwOp.fd, buf, op->rwOp.count, op->offset, reqId);

  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

ssize_t handle_allocated_pread_retry(uint64_t reqId, void** bufPtr) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct allocatedPreadOpPacked>(gServMngPtr->reqRingMap[reqId][0]);
  // fs_free(buf);
  *bufPtr = (void *)fs_malloc(op->rwOp.count + 1);
  memset(*bufPtr, 0, op->rwOp.count + 1);
  auto ret = fs_allocated_pread_retry(op->rwOp.fd, *bufPtr, op->rwOp.count, op->offset, reqId, bufPtr);
  
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: Error handling pending
int handle_unlink_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct unlinkOp>(gServMngPtr->reqRingMap[reqId][0]);
  int ret = fs_unlink_retry(op->path, reqId);
  return ret;
}

// TODO: Error handling pending
int handle_mkdir_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct mkdirOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_mkdir_retry(op->pathname, op->mode, reqId);
  return ret;
}

// TODO: Error handling pending
int handle_stat_retry(uint64_t reqId, struct stat* statbuf) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct statOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_stat_retry(op->path, statbuf, reqId);
  std::cout << "[DEBUG] In handle_stat retry, size = " << statbuf->st_size << std::endl;
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: Error handling pending
int handle_fstat_retry(uint64_t reqId, struct stat* statbuf) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct fstatOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_fstat_retry(op->fd, statbuf, reqId);
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: Error handling pending
int handle_open_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct openOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_open_retry(op->path, op->flags, op->mode, reqId);
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: Error handling pending
int handle_close_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct closeOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_close_retry(op->fd, reqId);
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: Error handling pending
int handle_opendir_retry(uint64_t reqId, CFS_DIR *dir) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct opendirOp>(gServMngPtr->reqRingMap[reqId][0]);
  *dir = *fs_opendir_retry(op->name, reqId);
  
  // clean up
  gServMngPtr->queueMgr->dequePendingMsg(reqId);
  return 0;
}

// int handle_rename_retry(uint64_t reqId) {
//   auto op = gServMngPtr->queueMgr->getPendingXreq<struct renameOp>(gServMngPtr->reqRingMap[reqId][0]);
//   auto ret = fs_rename_retry(op->oldpath, op->newpath, reqId);
//   if (ret > 0) {
//     // clean up
//     gServMngPtr->queueMgr->dequePendingMsg(reqId);
//   }

//   return ret;
// }

int handle_fsync_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct fsyncOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_fsync_retry(op->fd, reqId);
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

int handle_fdatasync_retry(uint64_t reqId) {
  auto op = gServMngPtr->queueMgr->getPendingXreq<struct fsyncOp>(gServMngPtr->reqRingMap[reqId][0]);
  auto ret = fs_fdatasync_retry(op->fd, reqId);
  if (ret > 0) {
    // clean up
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
  }

  return ret;
}

// TODO: fs_rename
int fs_retry_pending_ops(void *buf = nullptr, struct stat *statbuf = nullptr, 
  CFS_DIR *dir = nullptr, void **bufPtr = nullptr) {
  if (gServMngPtr->reqRingMap.size() == 0) {
    // std::cout << "[INFO] No ops to retry" << std::endl;
    return 0;
  }
 
  // std::cout << "[INFO] Checking connection with server" << std::endl;
  auto ret = check_primary_server_availability();
  if (ret == -1) {
    print_server_unavailable(__func__);
    return -1;
  } else {
    // std::cout << "[INFO] Connection to server successful" << std::endl; 
    for (auto itr = gServMngPtr->reqRingMap.begin(); itr != gServMngPtr->reqRingMap.end();) {
      // std::cout << "[INFO] Retrying request #" << itr->first << std::endl;
      auto reqId = itr->first;
      auto type = gServMngPtr->queueMgr->getMessageType(gServMngPtr->reqRingMap[reqId][0]);
      switch(type) {
        case CFS_OP_PWRITE:
          ret = handle_pwrite_retry(reqId);
          itr++;
          break;
        case CFS_OP_ALLOCED_PWRITE:
          ret = handle_allocated_pwrite_retry(reqId);
          itr++;
          break;
        case CFS_OP_CREATE:
          ret = handle_create_retry(reqId);
          itr++;
          break;
        case CFS_OP_PREAD:
          ret = handle_pread_retry(reqId, buf);
          if (ret > 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_ALLOCED_PREAD:
          ret = handle_allocated_pread_retry(reqId, bufPtr);
          if (ret > 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_UNLINK:
          ret = handle_unlink_retry(reqId);
          itr++;
          break;
        case CFS_OP_MKDIR:
          ret = handle_mkdir_retry(reqId);
          itr++;
          break;
        case CFS_OP_STAT:
          ret = handle_stat_retry(reqId, statbuf);
          if (ret == 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_FSTAT:
          ret = handle_fstat_retry(reqId, statbuf);
          if (ret == 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_OPEN:
          ret = handle_open_retry(reqId);
          if (ret > 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_CLOSE:
          ret = handle_close_retry(reqId);
          if (ret > 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        case CFS_OP_OPENDIR:
          ret = handle_opendir_retry(reqId, dir);
          if (ret == 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;
        // TODO: Check if rename needs to be flushed
        // case CFS_OP_RENAME:
        //   ret = handle_rename_retry(reqId);
        //   if (ret > 0) {
        //     gServMngPtr->reqRingMap.erase(itr++);
        //   } else {
        //     itr++;
        //   }
        //   break;
        case CFS_OP_FSYNC:
          ret = handle_fsync_retry(reqId);
          if (ret > 0) {
            gServMngPtr->reqRingMap.erase(itr++);
          } else {
            itr++;
          }
          break;

        default:
          std::cout << "[ERR] Not supported" << std::endl;
          itr++;
          break;
      }
    }

    return ret;
  }
}

/* #endregion fs util */

/* #region fs functions */
// TODO: Deprecate this, as we need atleast 2 keys to work
// with client retries (1 for server and 1 for private pending ops tracking)
// If app know's its key, allow init with the key
int fs_init(key_t key) {
  print_app_zc_mimic();
  if (gServMngPtr != nullptr) {
    fprintf(stderr, "ERROR fs has initialized\n");
    return -1;
  }

  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      try {
        gServMngPtr = new FsLibServiceMng();
        gServMngPtr->primaryServ = new FsService(gPrimaryServWid, key);
        gServMngPtr->primaryServ->inUse = true;
        // gPrimaryServ = new FsService(gPrimaryServWid, key);
      } catch (const char *msg) {
        fprintf(stderr, "Cannot init FsService:%s\n", msg);
        return -1;
      }

      gServMngPtr->multiFsServMap.insert(
          std::make_pair(gPrimaryServWid, gServMngPtr->primaryServ));
      gServMngPtr->multiFsServNum = 1;

      if (gLibSharedContext == nullptr) {
        gLibSharedContext = new FsLibSharedContext();
        gLibSharedContext->tidIncr = 1;
        gLibSharedContext->key = key;
      }

      gMultiFsServLock.clear(std::memory_order_release);
    }
  }
  return 0;
}

// NOTE: guarded by gMultiFsServLock
int fs_init_after_registration() {
#ifdef UFS_SOCK_LISTEN
  if (g_registered_max_num_worker <= 0) return -1;
  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      if (g_registered_max_num_worker > 0) {
        gServMngPtr = new FsLibServiceMng();
        int curWid;
        for (int i = 0; i < g_registered_max_num_worker; i++) {
          auto key =
              g_registered_shm_key_base + i * g_registered_shmkey_distance;
          fprintf(stdout,
                  "fs_init_after_registration-> init for key:%d map key is: %d "
                  "dist:%d\n",
                  key, (i + gPrimaryServWid), g_registered_shmkey_distance);
          curWid = i + gPrimaryServWid;
          gServMngPtr->multiFsServMap.insert(
              std::make_pair(curWid, new FsService(curWid, key)));
        }
        gServMngPtr->primaryServ = gServMngPtr->multiFsServMap[gPrimaryServWid];
        gServMngPtr->primaryServ->inUse = true;
        gServMngPtr->multiFsServNum = g_registered_max_num_worker;

        if (gLibSharedContext == nullptr) {
          gLibSharedContext = new FsLibSharedContext();
          gLibSharedContext->tidIncr = 1;
          gLibSharedContext->key = g_registered_shm_key_base;
        }

        gCleanedUpDone = false;
      }
      gMultiFsServLock.clear(std::memory_order_release);
    }
  }
#endif
  return 0;
}

// Use for app do not know its' key. which is for real application
int fs_register(void) {
#ifdef UFS_SOCK_LISTEN
  std::call_once(g_register_flag, fs_register_via_socket);
  // fprintf(stdout, "g_register_shm_key_base:%d max_nw:%d\n",
  //        g_registered_shm_key_base, g_registered_max_num_worker);
  int rt = fs_init_after_registration();
  return rt;
#endif
  return -1;
}

// The last key will be used for the pending queue
// each key will represent one FSP thread (instance/worker)
int fs_init_multi(int num_key, const key_t *keys) {
  print_app_zc_mimic();
  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      gServMngPtr = new FsLibServiceMng();
      int curWid;
      gServMngPtr->queueMgr = new PendingQueueMgr(keys[num_key - 1]);
      for (int i = 0; i < num_key - 1; i++) {
        auto key = keys[i];
        fprintf(stdout, "fs_init_multi-> init for key:%d map key is: %d\n", key,
                (i + gPrimaryServWid));
        curWid = i + gPrimaryServWid;
        gServMngPtr->multiFsServMap.insert(
            std::make_pair(curWid, new FsService(curWid, key)));
      }
      gServMngPtr->primaryServ = gServMngPtr->multiFsServMap[gPrimaryServWid];
      gServMngPtr->primaryServ->inUse = true;

      if (get_server_pid() != 0) {
        std::cout << "[ERR] did not receive server pid" << std::endl;
        gMultiFsServLock.clear(std::memory_order_release);
        return -1;
      }

      // NOTE: 1 key is used for private shm (pending ops)
      gServMngPtr->multiFsServNum = num_key - 1; 

      if (gLibSharedContext == nullptr) {
        gLibSharedContext = new FsLibSharedContext();
        gLibSharedContext->tidIncr = 1;
        gLibSharedContext->key = keys[0];
      }

      gCleanedUpDone = false;
      gMultiFsServLock.clear(std::memory_order_release);
    }
  }

  return 0;
}

// Now, it will busy wait the result by checking the op's status variable
void spinWaitOpDone(struct clientOp *copPtr) {
  // std::cout << "spinwaitdone init:" << (int) copPtr->opStatus << std::endl;
  while (copPtr->opStatus != OP_DONE) {
    // spin wait
  }
  // assert(copPtr->opStatus == OP_DONE);
}

//// Check if the access of fs has be initialized for calling process.
//// Return true when access is OK
static inline bool check_fs_access_ok() {
  // return !(gFsServ == nullptr && gMultiFsServNum == 0);
  return gServMngPtr->multiFsServNum != 0;
}

int fs_stat_internal(FsService *fsServ, const char *pathname,
                     struct stat *statbuf, uint64_t requestId) {
  struct shmipc_msg msg;
  struct statOp *statOp;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  statOp = (struct statOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_statOp(&msg, statOp, pathname, requestId);
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct statOp>(statOp, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    threadFsTid = 0;
    ret = fs_retry_pending_ops(nullptr, statbuf);
    std::cout << "[DEBUG] ret = " << ret << std::endl;
    std::cout << "[DEBUG] In fs_stat_internal retry, size = " << statbuf->st_size << std::endl;
    goto cleanup;
  }

  ret = statOp->ret;
end:
  if (ret != 0) goto cleanup;

  // copy the stats
  memcpy(statbuf, &(statOp->statbuf), sizeof(*statbuf));
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_stat(%s) ret:%d\n", pathname, ret);
#endif

cleanup:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_stat_internal_common(const char *pathname, struct stat *statbuf, uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_stat(pathname, requestId);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_STAT);
#endif
  int delixArr[32];
  int dummy;
  char *standardPath = filepath2TokensStandardized(pathname, delixArr, dummy);

retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  int rc = fs_stat_internal(service, standardPath, statbuf, requestId);

  if (rc >= 0) goto free_and_return;
  if (handle_inode_in_transfer(rc)) goto retry;

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    goto retry;
  }

free_and_return:
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_STAT, tsIdx);
#endif
  if (rc != 0) {
    errno = -rc;
    rc = -1;
  }
  std::cout << "[DEBUG] rc = " << rc << std::endl;
  return rc;
}

int fs_stat(const char *pathname, struct stat *statbuf) {
  auto requestId = getNewRequestId();
  return fs_stat_internal_common(pathname, statbuf, requestId);
} 

int fs_stat_retry(const char *pathname, struct stat *statbuf, uint64_t requestId) {
  return fs_stat_internal_common(pathname, statbuf, requestId);
}

int fs_fstat_internal(FsService *fsServ, int fd, struct stat *statbuf, uint64_t requestId) {
  struct shmipc_msg msg;
  struct fstatOp *fstatOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  fstatOp = (struct fstatOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_fstatOp(&msg, fstatOp, fd, requestId);
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct fstatOp>(fstatOp, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    threadFsTid = 0;
    ret = fs_retry_pending_ops(nullptr, statbuf);
    goto cleanup;
  }

  ret = fstatOp->ret;
end:
  if (ret != 0) goto cleanup;

  memcpy(statbuf, &(fstatOp->statbuf), sizeof(struct stat));
cleanup:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// TODO: Fix, when widIt not know, go with primary
int fs_fstat_internal_common(int fd, struct stat *statbuf, uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_fstat(fd, requestId);
#endif
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  FsService *fsServ; 
  if (widIt == gLibSharedContext->fdWidMap.end()) {
    fsServ = gServMngPtr->primaryServ;
  } else {
    fsServ = gServMngPtr->multiFsServMap[widIt->second];
  }
    
  return fs_fstat_internal(fsServ, fd, statbuf, requestId);
  // return -1;
}

int fs_fstat(int fd, struct stat *statbuf) {
  auto requestId = getNewRequestId();
  return fs_fstat_internal_common(fd, statbuf, requestId);
}

int fs_fstat_retry(int fd, struct stat *statbuf, uint64_t requestId) {
  return fs_fstat_internal_common(fd, statbuf, requestId);
}

static int fs_open_internal(FsService *fsServ, const char *path, int flags,
                            mode_t mode, uint64_t requestId, 
                            uint64_t *size = nullptr) {
  struct shmipc_msg msg;
  struct openOp *oop;
  off_t ring_idx;
  int ret;

  if (!check_fs_access_ok()) {
    std::cerr << "ERROR fs access uninitialized" << std::endl;
    return FS_ACCESS_UNINIT_ERROR;
  }

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  oop = (struct openOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_openOp(&msg, oop, path, flags, mode, size, requestId);

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct openOp>(oop, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  // TODO make sure this doesn't get optimized out?
  ret = oop->ret;

  // TODO not sure what this is doing
  if (size) *size = oop->size;

end:
  if (msg.type == CFS_OP_CREATE && gServMngPtr->reqRingMap.count(requestId) == 0) {
    auto pendingOpIdx = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
    gServMngPtr->queueMgr->enqueuePendingXreq<struct openOp>(oop, pendingOpIdx);
    gServMngPtr->reqRingMap[requestId].push_back(pendingOpIdx);
  }

#ifndef CFS_LIB_LDB
  if (ret >= 0) {
    auto curLock = &(gLibSharedContext->fdFileHandleMapLock);
    curLock->lock();
    // try to find if the fileName has already recorded
    struct FileHandle *curHandle = nullptr;
    auto fnameFhIt = gLibSharedContext->fnameFileHandleMap.find(oop->path);
    if (fnameFhIt != gLibSharedContext->fnameFileHandleMap.end()) {
      curHandle = fnameFhIt->second;
      curHandle->refCount++;
    }

    if (curHandle == nullptr) {
      // not found fileName, create new fileHandle
      curHandle = new FileHandle();
      // We directly use ino as filehandle
      curHandle->id = EMBEDED_INO_FILED_OP_OPEN(oop);
      // fprintf(stdout, "open return fdhd:%d\n", curHandle->id);
      curHandle->refCount = 1;
      strcpy(curHandle->fileName, oop->path);
      gLibSharedContext->fnameFileHandleMap.emplace(oop->path, curHandle);
    }
#ifdef _CFS_LIB_PRINT_REQ_
    fprintf(stderr, "insert to fdFileHandleMap - fd:%d, curHandle:%p\n", ret,
            curHandle);
#endif
    gLibSharedContext->fdFileHandleMap.insert(std::make_pair(ret, curHandle));
    gLibSharedContext->fdOffsetMap[ret] = 0;
    // set the offset to 0
    gLibSharedContext->fdCurOffMap.emplace(ret, 0);
    curLock->unlock();
  }
#endif

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_open(%s, flags:%d) ret:%d\n", path, flags, ret);
#endif
  // TODO reduce the amount of time the slot is held
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// TODO [BENITA] Should set offset correctly
int fs_open_internal_common(const char *path, int flags, mode_t mode, uint64_t requestId) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPEN);
#endif
#ifdef LDB_PRINT_CALL
  print_open(path, flags, mode, false, requestId);
#endif
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // NOTE: the first time invoking this is going to be extremely costly, ~0.2s
  // So, it is important for any user (thread) to invoke this
  //(void) check_app_thread_mem_buf_ready();
  assert(threadFsTid != 0);
  
  // TODO: how to know if new file was created Only for create
retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  int rc = fs_open_internal(service, standardPath, flags, mode, requestId, nullptr);

  if (rc > 0) {
    gLibSharedContext->fdWidMap[rc] = wid;
    goto free_and_return;
  } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
    goto free_and_return;
  }

  // migration related errors
  if (handle_inode_in_transfer(rc)) {
    goto retry;
  }

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    goto retry;
  }

free_and_return:
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPEN, tsIdx);
#endif
  return rc;
}

int fs_open(const char *path, int flags, mode_t mode) {
  auto requestId = getNewRequestId();
  return fs_open_internal_common(path, flags, mode, requestId);
}

int fs_open_retry(const char *path, int flags, mode_t mode, uint64_t requestId) {
  return fs_open_internal_common(path, flags, mode, requestId);
}

int fs_open2(const char *path, int flags) { return fs_open(path, flags, 0); }

int fs_close_internal(FsService *fsServ, int fd, uint64_t requestId) {
  struct shmipc_msg msg;
  struct closeOp *cloOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  cloOp = (struct closeOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_closeOp(&msg, cloOp, fd, requestId);
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
    ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct closeOp>(cloOp, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = cloOp->ret;

end:
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_close(fd:%d) ret:%d\n", fd, ret);
#endif
  if (ret == 0) {
    auto curLock = &(gLibSharedContext->fdFileHandleMapLock);
    curLock->lock();
    auto fileHandleIt = gLibSharedContext->fdFileHandleMap.find(fd);
    if (fileHandleIt != gLibSharedContext->fdFileHandleMap.end()) {
      if (fileHandleIt->second->refCount > 1) {
        // we only delete [fd<->fileHandle] record if this is not the last FD
        // that is referring to that file. Then we can still find the cache
        // items once the file is re-opened.
        // TODO (jingliu): delete cache once unlink() is called
        // ==> needs to free the memory used by page cache
        gLibSharedContext->fdFileHandleMap.erase(fd);
      }
    }
    curLock->unlock();
  }
  // TODO (jingliu): it looks no need to delete it
  // Let's revisit it when doing experiments.
  // if (ret == 0) {
  //     fdWidMap.erase(fd);
  // }
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// TODO [BENITA] Should del inode<->offset mapping
int fs_close_internal_common(int fd, uint64_t requestId) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSE);
#endif
#ifdef LDB_PRINT_CALL
  print_close(fd, false, requestId);
#endif
retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  int rc = fs_close_internal(service, fd, requestId);

  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSE, tsIdx);
#endif
  return rc;
}

int fs_close(int fd) {
  auto requestId = getNewRequestId();
  return fs_close_internal_common(fd, requestId);
}

int fs_close_retry(int fd, uint64_t requestId) {
  return fs_close_internal_common(fd, requestId);
}

int fs_unlink_internal(FsService *fsServ, const char *pathname, uint64_t reqId) {
#ifdef LDB_PRINT_CALL
  print_unlink(pathname);
#endif
  struct shmipc_msg msg;
  struct unlinkOp *unlkop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  unlkop = (struct unlinkOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_unlinkOp(&msg, unlkop, pathname, reqId);

  if (gServMngPtr->reqRingMap.count(reqId) == 0) {
    auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
    gServMngPtr->queueMgr->enqueuePendingXreq<struct unlinkOp>(unlkop, off);
    gServMngPtr->reqRingMap[reqId].push_back(off);
  }

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = unlkop->ret;

end:
  // cleanup
  if (ret != 0) {
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
    gServMngPtr->reqRingMap.erase(reqId);
  }
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_unlink(pathname:%s) return:%d\n", pathname, ret);
#endif
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// Man page of unlink:
// unlink() deletes a name from the filesystem.  If that name was the last link
// to a file and no processes have the file open, the file is deleted and the
// space it was using is made available for reuse.
int fs_unlink_internal_common(const char *pathname, uint64_t requestId) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_UNLINK);
#endif

  char *standardPath = filepath2TokensStandardized(pathname, delixArr, dummy);
  // NOTE: For now, unlink will always go to the primary worker
  int rt = fs_unlink_internal(gServMngPtr->primaryServ, standardPath, requestId);
  if (rt == 0) {
    // unlink succeed, remove path from pathWidmap
    updatePathWidMap(/*newWid*/ 0, standardPath);
    auto it =
        gLibSharedContext->pathLDBLeaseMap.find(std::string(standardPath));
    if (it != gLibSharedContext->pathLDBLeaseMap.end()) {
      delete it->second;
      gLibSharedContext->pathLDBLeaseMap.unsafe_erase(it);
    }
  }

  if (rt < 0 && rt >= FS_REQ_ERROR_INODE_IN_TRANSFER) rt = -1;
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_UNLINK, tsIdx);
#endif
  return rt;
}

int fs_unlink(const char *pathname) {
  auto requestId = getNewRequestId();
  return fs_unlink_internal_common(pathname, requestId);
}

int fs_unlink_retry(const char *pathname, uint64_t requestId) {
  return fs_unlink_internal_common(pathname, requestId);
}

int fs_mkdir_internal(FsService *fsServ, const char *pathname, mode_t mode, 
  uint64_t reqId, bool isRetry) {
  struct shmipc_msg msg;
  struct mkdirOp *mop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  mop = (struct mkdirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_mkdirOp(&msg, mop, pathname, mode, isRetry, reqId);

  if (gServMngPtr->reqRingMap.count(reqId) == 0) {
    auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
    gServMngPtr->queueMgr->enqueuePendingXreq<struct mkdirOp>(mop, off);
    gServMngPtr->reqRingMap[reqId].push_back(off);
  }

  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
    ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = mop->ret;
end:
  if (ret < 0) {
    gServMngPtr->queueMgr->dequePendingMsg(reqId);
    gServMngPtr->reqRingMap.erase(reqId);
  }
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_mkdir(%s) ret:%d\n", pathname, ret);
#endif
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_mkdir_internal_common(const char *pathname, mode_t mode, uint64_t requestId, bool isRetry) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_MKDIR);
#endif
  
#ifdef LDB_PRINT_CALL
  print_mkdir(pathname, mode, requestId);
#endif
  int rt = fs_mkdir_internal(gServMngPtr->primaryServ, pathname, mode, requestId, isRetry);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_MKDIR, tsIdx);
#endif
  return rt;
}

int fs_mkdir(const char *pathname, mode_t mode) {
  auto requestId = getNewRequestId();
  return fs_mkdir_internal_common(pathname, mode, requestId, false /*isRetry*/);
}

int fs_mkdir_retry(const char *pathname, mode_t mode, uint64_t requestId) {
  return fs_mkdir_internal_common(pathname, mode, requestId, true /*isRetry*/);
}

CFS_DIR *fs_opendir_internal(FsService *fsServ, const char *name, uint64_t requestId) {
  struct shmipc_msg msg;
  struct opendirOp *odop;
  cfs_dirent *cfsDentryPtr;
  CFS_DIR *dirp;
  off_t ring_idx;
  int numTotalDentry;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // zalloc is unnecessarily costly, use malloc
  void *dataPtr = fs_malloc((FS_DIR_MAX_WIDTH) * sizeof(cfs_dirent));

  int err = 0;
  threadMemBuf->getBufOwnerInfo(dataPtr, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_opendir_internal: Error in getBufOwnerInfo\n");
    return NULL;
  }

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  odop = (struct opendirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_opendirOp(&msg, odop, name, requestId);
  odop->alOp.shmid = shmid;
  odop->alOp.dataPtrId = dataPtrId;

  // send request
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
    ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    fs_free(dataPtr);
    threadFsTid = 0;

    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct opendirOp>(odop, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    
    CFS_DIR* res = (CFS_DIR *)malloc(sizeof(*res));
    threadFsTid = 0;
    fs_retry_pending_ops(nullptr, nullptr, res); 
    if (res != nullptr) {
      std::cout << "res is not null" << std::endl;
    }
    return res;
  }

  // TODO ensure that the operation was successful
  numTotalDentry = odop->numDentry;
  cfsDentryPtr = (cfs_dirent *)(dataPtr);
  dirp = (CFS_DIR *)malloc(sizeof(*dirp));
  dirp->dentryNum = 0;
  dirp->dentryIdx = 0;
  if (numTotalDentry > 0) {
    dirp->firstDentry = (dirent *)malloc(sizeof(dirent) * numTotalDentry);
    for (int i = 0; i < numTotalDentry; i++) {
      if ((cfsDentryPtr + i)->inum == 0) {
        // Currently the openDir's FSP side implementation will read the whole
        // inode content, which contains the empty inode (after unlink)
        continue;
      }
      (dirp->firstDentry + dirp->dentryNum)->d_ino = (cfsDentryPtr + i)->inum;
      strcpy((dirp->firstDentry + dirp->dentryNum)->d_name,
             (cfsDentryPtr + i)->name);
      // Explicitly set d_type to UNKNOWN since we don't plan to carry type
      // info. See struct cfs_dirent comment.
      (dirp->firstDentry + dirp->dentryNum)->d_type = DT_UNKNOWN;
      dirp->dentryNum++;
    }
  }
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_opendir(%s)\n", name);
  
#endif
  // TODO measure cost to hold the slot. If it is too much, we might as well
  // copy the data and release the slot.
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  fs_free(dataPtr);
  return dirp;
}

CFS_DIR *fs_opendir_internal_common(const char *name, uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_opendir(name, requestId);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPENDIR);
#endif
  
  CFS_DIR *rt;
  rt = fs_opendir_internal(gServMngPtr->primaryServ, name, requestId);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPENDIR, tsIdx);
#endif
  return rt;
}

CFS_DIR *fs_opendir(const char *name) {
  auto requestId = getNewRequestId();
  return fs_opendir_internal_common(name, requestId);
}

CFS_DIR *fs_opendir_retry(const char *name, uint64_t requestId) {
  return fs_opendir_internal_common(name, requestId);
}

struct dirent *fs_readdir(CFS_DIR *dirp) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_readdir()\n");
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READDIR);
#endif
  struct dirent *dent = NULL;
  if (dirp->dentryIdx > dirp->dentryNum) goto return_dent;
  if (dirp->dentryIdx == dirp->dentryNum) {
    goto return_dent;
  }
  dent = (dirp->firstDentry + (dirp->dentryIdx++));
return_dent:
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READDIR, tsIdx);
#endif
  return dent;
}

int fs_closedir(CFS_DIR *dirp) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_closedir(dirp:%p)\n", dirp);
#endif
  if (dirp == nullptr) {
    return -1;
  }

#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSEDIR);
#endif

  // assume a close must follow an open, then free in close
  if (dirp->firstDentry != NULL) {
    free(dirp->firstDentry);
  }

  free(dirp);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSEDIR, tsIdx);
#endif
  return 0;
}

// TODO: Note - Not being used
int fs_rmdir_internal(FsService *fsServ, const char *pathname) {
  struct shmipc_msg msg;
  struct rmdirOp *rmdOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  rmdOp = (struct rmdirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_rmdirOp(&msg, rmdOp, pathname);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = rmdOp->ret;
end:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_rmdir(const char *pathname) {
  // return fs_rmdir_internal(gServMngPtr->primaryServ, pathname);
  // TODO (jingliu): implement a real rmdir here.
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_RMDIR);
#endif
  auto reqId = getNewRequestId();
  int rt = fs_unlink_internal(gServMngPtr->primaryServ, pathname, reqId);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_rmdir(%s) ret:%d\n", pathname, rt);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_RMDIR, tsIdx);
#endif
  return rt;
}

int fs_rename_internal(FsService *fsServ, const char *oldpath,
                       const char *newpath) {
  struct shmipc_msg msg;
  struct renameOp *rnmOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  rnmOp = (struct renameOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_renameOp(&msg, rnmOp, oldpath, newpath);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = rnmOp->ret;
end:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_rename(from:%s, to:%s) return %d\n", oldpath, newpath,
          ret);
#endif
  return ret;
}

// TODO: Add to pending
int fs_rename(const char *oldpath, const char *newpath) {
#ifdef LDB_PRINT_CALL
  print_rename(oldpath, newpath);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_RENAME);
#endif
  int rt = fs_rename_internal(gServMngPtr->primaryServ, oldpath, newpath);
  if (rt < 0 && rt >= FS_REQ_ERROR_INODE_IN_TRANSFER) {
    errno = -rt;
    rt = -1;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_RENAME, tsIdx);
#endif
  return rt;
}

int fs_fsync_internal(FsService *fsServ, int fd, bool isDataSync, uint64_t requestId) {
  struct shmipc_msg msg;
  struct fsyncOp *fsyncOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  fsyncOp = (struct fsyncOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_fsyncOp(&msg, fsyncOp, fd, isDataSync);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
    ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct fsyncOp>(fsyncOp, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
    goto end;
  }

  ret = fsyncOp->ret;
end:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_fsync_internal_common(int fd, uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif

retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_fsync_internal(service, fd, false, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fsync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}

int fs_fsync(int fd) {
  auto requestId = getNewRequestId();
  return fs_fsync_internal_common(fd, requestId);
}

int fs_fsync_retry(int fd, uint64_t requestId) {
  return fs_fsync_internal_common(fd, requestId);
}

int fs_fdatasync_internal_common(int fd, uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif

retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_fsync_internal(service, fd, true, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fdatasync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}

int fs_fdatasync(int fd) {
  auto requestId = getNewRequestId();
  return fs_fdatasync_internal_common(fd, requestId);
}

int fs_fdatasync_retry(int fd, uint64_t requestId) {
  return fs_fdatasync_internal_common(fd, requestId);
}

// TODO: Support pending
// template<bool UNLINK_ONLY> with -std=c++17, if constexpr can be used instead.
// For now, given the amount of reuse, I think we can let a single if statement
// check happen. Especially since syncall will do i/o.
// Once we move to c++17, this would resolve at compile time.
void generic_fs_syncall(bool UNLINK_ONLY) {
  using item_t = std::pair<FsService *, off_t>;
  // TODO: consider using a fixed size array if speed is an issue
  // Or maybe a custom stack allocator for the vector.
  // TODO: change signature to return success/error
  std::vector<item_t> async_ops;
  struct shmipc_msg msg;

  for (auto it : gServMngPtr->multiFsServMap) {
    if (!(it.second->inUse)) continue;

    auto serv = it.second;
    off_t ring_idx = shmipc_mgr_alloc_slot_dbg(serv->shmipc_mgr);
    memset(&msg, 0, sizeof(msg));
    if /*constexpr*/ (UNLINK_ONLY) {
      auto synop =
          (struct syncunlinkedOp *)IDX_TO_XREQ(serv->shmipc_mgr, ring_idx);
      prepare_syncunlinkedOp(&msg, synop);
    } else {
      auto synop = (struct syncallOp *)IDX_TO_XREQ(serv->shmipc_mgr, ring_idx);
      prepare_syncallOp(&msg, synop);
    }

    shmipc_mgr_put_msg_nowait(serv->shmipc_mgr, ring_idx, &msg, 
     shmipc_STATUS_READY_FOR_SERVER);
    async_ops.emplace_back(serv, ring_idx);
  }

  for (auto &it : async_ops) {
    off_t ring_idx;
    FsService *serv;
    std::tie(serv, ring_idx) = it;

    memset(&msg, 0, sizeof(msg));
    shmipc_mgr_wait_msg(serv->shmipc_mgr, ring_idx, &msg);
    shmipc_mgr_dealloc_slot(serv->shmipc_mgr, ring_idx);
    if (msg.retval != 0) {
      if /*constexpr*/ (UNLINK_ONLY) {
        fprintf(stderr, "syncunlinked failed on a worker\n");
      } else {
        fprintf(stderr, "syncall failed on a worker\n");
      }
    }
  }
}

// TODO: Support pending
void fs_syncall() {
  // generic_fs_syncall</*UNLINK_ONLY*/false>();
  generic_fs_syncall(/*UNLINK_ONLY*/ false);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_syncall()\n");
#endif
}

// TODO: Support pending
void fs_syncunlinked() {
  // generic_fs_syncall</*UNLINK_ONLY*/true>();
  generic_fs_syncall(/*UNLINK_ONLY*/ true);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_syncunlinked()\n");
#endif
}

// TODO: Note - not being used
ssize_t fs_read_internal(FsService *fsServ, int fd, void *buf, size_t count) {
  ssize_t rc;
  /*
   * TODO change the code for read/write to resemble
   * while remaining > 0; do ....; done
   * That way we don't have to have an if and else condition.
   * and we keep decrementing by RING_DATA_ITEM_SIZE
   */

  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct readOpPacked *rop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    rop_p = (struct readOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_readOp(&msg, rop_p, fd, count);
    if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
      &msg, gServMngPtr->fsServPid) == -1) {
      print_server_unavailable(__func__);
      threadFsTid = 0;
      rc = fs_retry_pending_ops(buf);
      goto end;
    }

    rc = rop_p->rwOp.ret;
  end:
    // NOTE: FIXME This doesn't look right. If MIMIC_APP_ZC is defined, then we
    // don't copy into buf? That would mean that it is still in the ring
    // data region but can be overwritten by anyone after we dealloc the
    // slot.
    #ifndef MIMIC_APP_ZC
        void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    #endif
    // copy the data back to user
    #ifndef MIMIC_APP_ZC
        if (rc > 0) {
          memcpy(buf, curDataPtr, count);
        }

        shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    #endif
    } else {
      rc = -1;
    }
    #ifdef _CFS_LIB_PRINT_REQ_
      fprintf(stdout, "fs_read(fd:%d count:%lu) ret:%ld\n", fd, count, rc);
    #endif
    return rc;
}

static ssize_t fs_pread_internal(FsService *fsServ, int fd, void *buf,
                                 size_t count, off_t offset, uint64_t requestId) {
  ssize_t rc;
#ifdef _CFS_LIB_PRINT_REQ_
  pid_t curPid = syscall(__NR_gettid);
  fprintf(stdout, "fs_pread(fd:%d, count:%ld, offset:%lu pid:%d)\n", fd, count,
          offset, curPid);
#endif
  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct preadOpPacked *prop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_preadOp(&msg, prop_p, fd, count, offset);
    // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
    if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
      ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
      if (gServMngPtr->reqRingMap.count(requestId) == 0) {
        auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
        gServMngPtr->queueMgr->enqueuePendingXreq<struct preadOpPacked>(prop_p, off);
        gServMngPtr->reqRingMap[requestId].push_back(off);
      }
      
      print_server_unavailable(__func__);
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
      threadFsTid = 0;
      return fs_retry_pending_ops(buf);
    }

    rc = prop_p->rwOp.ret;

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
#endif
    // copy the data back to user
#ifndef MIMIC_APP_ZC
    if (rc > 0) {
      memcpy(buf, curDataPtr, count);
    }
#endif
  
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  } else {
    // For now, this only support < RING_DATA_ITEM_SIZE
    rc = -1;
  }
  return rc;
}

ssize_t fs_read_internal_common(int fd, void *buf, size_t count, uint64_t requestId) {
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READ);
#endif

#ifdef LDB_PRINT_CALL
  print_read(fd, buf, count, requestId);
#endif

retry:
  auto service = getFsServiceForFD(fd, wid);
  // ssize_t rc = fs_read_internal(service, fd, buf, count);
  auto prevOffset = service->getOffset(fd);
  ssize_t rc = fs_pread_internal(service, fd, buf, count, prevOffset, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) {
      goto retry;
    }
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) {
      goto retry;
    }
  } else {
    service->updateOffset(fd, prevOffset + rc - 1);
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READ, tsIdx);
#endif
  return rc;
}

ssize_t fs_read(int fd, void *buf, size_t count) {
  auto requestId = getNewRequestId();
  return fs_read_internal_common(fd, buf, count, requestId);
}

ssize_t fs_read_retry(int fd, void *buf, size_t count, uint64_t requestId) {
  return fs_read_internal_common(fd, buf, count, requestId);
}

ssize_t fs_pread_internal_common(int fd, void *buf, size_t count, off_t offset, uint64_t requestId) {
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif

#ifdef LDB_PRINT_CALL
  print_pread(fd, buf, count, offset, requestId);
#endif

retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_pread_internal(service, fd, buf, count, offset, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) {
      goto retry;
    }
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) {
      goto retry;
    }
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif

  return rc;
}

ssize_t fs_pread(int fd, void *buf, size_t count, off_t offset) {
  auto requestId = getNewRequestId();
  return fs_pread_internal_common(fd, buf, count, offset, requestId);
}

ssize_t fs_pread_retry(int fd, void *buf, size_t count, off_t offset, uint64_t requestId) {
  return fs_pread_internal_common(fd, buf, count, offset, requestId);
}

// TODO: Add to pending
// bcause the regular file named by path or referenced by fd to be truncated
// to a size of precisely length bytes
int fs_ftruncate(int fd, off_t length) { return 0; }

// TODO: Add to pending
// doAllocate() allows the caller to directly manipulate the allocated disk
// space for the file referred to  by  fd for the byte range starting at offset
// and continuing for len bytes
int fs_fallocate(int fd, int mode, off_t offset, off_t len) { return 0; }

// TODO: Note - not being used
static ssize_t fs_write_internal(FsService *fsServ, int fd, const void *buf,
                                 size_t count) {
  ssize_t rc;
  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct writeOpPacked *wop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    wop_p = (struct writeOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_writeOp(&msg, wop_p, fd, count);

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    memcpy(curDataPtr, buf, count);
#endif

    // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
    if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
      &msg, gServMngPtr->fsServPid) == -1) {
      print_server_unavailable(__func__);
      threadFsTid = 0;
      rc = fs_retry_pending_ops();
    }

    rc = wop_p->rwOp.ret;
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  } else {
    // NOTE: let's first issue the request one by one to see how bad we can be.
    rc = 0;
    size_t bytes = 0;
    size_t toWrite = count;
    int numNeedSlot = (count - 1) / (RING_DATA_ITEM_SIZE) + 1;
    //    fprintf(stdout, "fs_write, count:%ld numNeedSlot %d\n", count,
    //    numNeedSlot);
    for (int i = 0; i < numNeedSlot; i++) {
      struct shmipc_msg msg;
      struct writeOpPacked *wop_p;
      off_t ring_idx;

      int tmpBytes;
      if (toWrite > RING_DATA_ITEM_SIZE) {
        tmpBytes = RING_DATA_ITEM_SIZE;
      } else {
        tmpBytes = toWrite;
      }

      memset(&msg, 0, sizeof(struct shmipc_msg));
      ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
      wop_p = (struct writeOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
      prepare_writeOp(&msg, wop_p, fd, tmpBytes);

#ifndef MIMIC_APP_ZC
      void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
      memcpy(curDataPtr, ((char *)buf + bytes), tmpBytes);
#endif

      // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
      if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
        &msg, gServMngPtr->fsServPid) == -1) {
        print_server_unavailable(__func__);
        threadFsTid = 0;
        rc = fs_retry_pending_ops();
      }
      

      rc += wop_p->rwOp.ret;
      if (wop_p->rwOp.ret < 0) {
        shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
        return -1;
      }
      bytes += tmpBytes;
      toWrite -= tmpBytes;
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    }
  }
#ifdef _CFS_LIB_PRINT_REQ_
  // fprintf(stdout, "fs_write(fd:%d) ret:%ld\n", fd, rc);
  // std::cout << "fs_write: tid:" << std::this_thread::get_id() << std::endl;
#endif
  return rc;
}

static ssize_t fs_pwrite_internal(FsService *fsServ, int fd, const void *buf,
                                  size_t count, off_t offset, uint64_t requestId) {
  ssize_t total_rc = 0;
  size_t bytes = 0;
  size_t toWrite = count;
  int numNeedSlot = (int)((count - 1) / (RING_DATA_ITEM_SIZE) + 1);

  for (int i = 0; i < numNeedSlot; i++) {
    struct shmipc_msg msg;
    struct pwriteOpPacked *pwop_p;
    off_t ring_idx;

    int tmpBytes;
    if (toWrite > RING_DATA_ITEM_SIZE) {
      tmpBytes = RING_DATA_ITEM_SIZE;
    } else {
      tmpBytes = toWrite;
    }

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    pwop_p = (struct pwriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_pwriteOp(&msg, pwop_p, fd, tmpBytes, offset, requestId);

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    memcpy(curDataPtr, ((char *)buf + bytes), tmpBytes);
#endif
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto pendingOpIdx = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct pwriteOpPacked>(pwop_p, pendingOpIdx);
      gServMngPtr->queueMgr->enqueuePendingData(curDataPtr, pendingOpIdx, tmpBytes);
      gServMngPtr->reqRingMap[requestId].push_back(pendingOpIdx);
    }
    
    // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
    if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, 
      ring_idx, &msg, gServMngPtr->fsServPid) == -1) {
      print_server_unavailable(__func__);
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
      threadFsTid = 0;
      return fs_retry_pending_ops(); 
    }

    ssize_t rc = pwop_p->rwOp.ret;
    if (rc < 0) {
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
      return rc;
    } 

    total_rc += rc;
    bytes += tmpBytes;
    toWrite -= tmpBytes;
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  }

  return total_rc;
}

ssize_t fs_pwrite_internal_common(int fd, const void *buf, size_t count, off_t offset, uint64_t requestId) {
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PWRITE);
#endif
  int wid = -1;
  
#ifdef LDB_PRINT_CALL
  print_pwrite(fd, buf, count, offset, requestId);
#endif

retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_pwrite_internal(service, fd, buf, count, offset, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) {
      goto retry;
    }
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry){
      goto retry;
    }
  } 
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PWRITE, tsIdx);
#endif
  return rc;
}

ssize_t fs_write_internal_common(int fd, const void *buf, size_t count,  uint64_t requestId) {
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_WRITE);
#endif
  
#ifdef LDB_PRINT_CALL
  print_write(fd, buf, count, requestId);
#endif
  int wid = -1;
retry:
  auto service = getFsServiceForFD(fd, wid);
  auto prevOffset = service->getOffset(fd);
  
  ssize_t rc = fs_pwrite_internal(service, fd, buf, count, prevOffset, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) {
      goto retry;
    }
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) {
      goto retry;
    }
  } else {
    service->updateOffset(fd, prevOffset + rc - 1);
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_WRITE, tsIdx);
#endif

  return rc;
}

ssize_t fs_write_retry(int fd, const void *buf, size_t count, uint64_t requestId) {
  return fs_write_internal_common(fd, buf, count, requestId);
}

ssize_t fs_write(int fd, const void *buf, size_t count) {
  auto requestId = getNewRequestId(); 
  return fs_write_internal_common(fd, buf, count, requestId);
}

ssize_t fs_pwrite_retry(int fd, const void *buf, size_t count, off_t offset, uint64_t requestId) {
  return fs_pwrite_internal_common(fd, buf, count, offset, requestId);
}

ssize_t fs_pwrite(int fd, const void *buf, size_t count, off_t offset) {
  auto requestId = getNewRequestId();
  return fs_pwrite_internal_common(fd, buf, count, offset, requestId);
}

/* #endregion */

/* #region lease */
OpenLeaseMapEntry *LeaseRef(const char *path) {
  assert(false);
  return nullptr;
}

OpenLeaseMapEntry *LeaseRef(int fd) {
  std::shared_lock<std::shared_mutex> guard(
      gLibSharedContext->openLeaseMapLock);
  auto it = gLibSharedContext->fdOpenLeaseMap.find(fd);
  if (it == gLibSharedContext->fdOpenLeaseMap.end()) {
    // base fd lease not found, go to server
    return nullptr;
  } else {
    it->second->ref += 1;
    return it->second;
  }
}

void LeaseUnref(OpenLeaseMapEntry *entry, bool del = false) {
  if (del) {
    assert(false);
  } else {
    std::shared_lock<std::shared_mutex> lock(
        gLibSharedContext->openLeaseMapLock);
    int ref = entry->ref.fetch_sub(1);
    if (ref == 1) entry->cv.notify_all();
  }
}

int fs_open_lease(const char *path, int flags, mode_t mode) {
  int delixArr[32];
  int dummy;
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // reference step
  gLibSharedContext->openLeaseMapLock.lock_shared();
  auto it = gLibSharedContext->pathOpenLeaseMap.find(standardPath);
  OpenLeaseMapEntry *entry = nullptr;
  if (it == gLibSharedContext->pathOpenLeaseMap.end()) {
    // open, install lease, local open
    gLibSharedContext->openLeaseMapLock.unlock_shared();
    // do open to the server
    assert(threadFsTid != 0);
    uint64_t size;
    auto requestId = getNewRequestId();

  retry:
    int wid = -1;
    auto service = getFsServiceForPath(standardPath, wid);
    int rc = fs_open_internal(service, standardPath, flags, mode, requestId, &size);

    if (rc > 0) {
      gLibSharedContext->fdWidMap[rc] = wid;
      goto open_end;
    } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
      goto open_end;
    }
    // migration related errors
    if (handle_inode_in_transfer(rc)) {
      goto retry;
    }
    wid = getWidFromReturnCode(rc);
    if (wid >= 0) {
      updatePathWidMap(wid, standardPath);
      goto retry;
    }

  open_end:
    if (rc > 0 && size != 0) {
      {
        // install lease & local_open
        // reference step
        std::unique_lock<std::shared_mutex> lock(
            gLibSharedContext->openLeaseMapLock);
        // TODO: check for concurrent opens
        OpenLeaseMapEntry *new_entry =
            new OpenLeaseMapEntry(rc, standardPath, size);
        auto retval = gLibSharedContext->pathOpenLeaseMap.emplace(standardPath,
                                                                  new_entry);
        assert(retval.second);
        auto retval2 = gLibSharedContext->fdOpenLeaseMap.emplace(rc, new_entry);
        assert(retval2.second);
        entry = new_entry;
        entry->ref++;
      }
      {
        // local_open
        std::unique_lock<std::shared_mutex> lock(entry->lock);
        rc = entry->lease->Open(flags, mode);
      }
      LeaseUnref(entry);
    }
    free(standardPath);
    return rc;
  } else {
    // local open
    // reference done
    bool is_create = flags & O_CREAT;
    if (!is_create) {
      entry = it->second;
      entry->ref++;
    }
    gLibSharedContext->openLeaseMapLock.unlock_shared();
    // do local_open
    if (is_create) {
      free(standardPath);
      return -1;
    }
    int fd = -1;
    {
      // local_open
      std::unique_lock<std::shared_mutex> lock(entry->lock);
      fd = entry->lease->Open(flags, mode);
    }
    LeaseUnref(entry);
    free(standardPath);
    return fd;
  }
}

int fs_close_lease(int fd) {
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    if (entry) {
      {
        // local close
        int offset = OpenLease::FindOffset(fd);
        std::unique_lock<std::shared_mutex> lock(entry->lock);
        entry->lease->Close(offset);
      }
      LeaseUnref(entry);
      return 0;
    } else {
      return fs_close(fd);
    }
  } else {
    // global fd, go to server
    return fs_close(fd);
  }
}
/*#endregion*/

// TODO: Fix allocated retry
/* #region fs_allocated_xxx */
// TODO: Note - not being used
static ssize_t fs_allocated_read_internal(FsService *fsServ, int fd, void *buf,
                                          size_t count) {
  struct shmipc_msg msg;
  struct allocatedReadOpPacked *arop_p;
  struct allocatedReadOp arop;
  off_t ring_idx;
  ssize_t rc;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_read_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  arop_p =
      (struct allocatedReadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_allocatedReadOp(&msg, arop_p, fd, count);
  arop_p->alOp.shmid = shmid;
  arop_p->alOp.dataPtrId = dataPtrId;

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    rc = fs_retry_pending_ops();
  }
  unpack_allocatedReadOp(arop_p, &arop);
  rc = arop_p->rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", arop.rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

static ssize_t fs_allocated_pread_internal(FsService *fsServ, int fd, void *buf,
  size_t count, off_t offset, uint64_t requestId, void **bufPtr) {
  struct shmipc_msg msg;
  struct allocatedPreadOpPacked *aprop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedPreadOp aprop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_pread_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  } else {
#ifdef _CFS_LIB_DEBUG_
    dumpAllocatedOpCommon(&aprop_p->alOp);
#endif
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  aprop_p = (struct allocatedPreadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                         ring_idx);
  prepare_allocatedPreadOp(&msg, aprop_p, fd, count, offset, requestId);
  aprop_p->alOp.shmid = shmid;
  aprop_p->alOp.dataPtrId = dataPtrId;

  // reset sequential number
  aprop_p->alOp.perAppSeqNo = 0;
  aprop_p->rwOp.realCount = 0;

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    if (gServMngPtr->reqRingMap.count(requestId) == 0) {
      auto off = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
      gServMngPtr->queueMgr->enqueuePendingXreq<struct allocatedPreadOpPacked>(aprop_p, off);
      gServMngPtr->reqRingMap[requestId].push_back(off);
    }
      
    print_server_unavailable(__func__);
    
    // cleanup
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    // fs_free(buf);
    threadFsTid = 0;

    auto ret = fs_retry_pending_ops(nullptr, nullptr, nullptr, bufPtr);
    return ret;
  }
  unpack_allocatedPreadOp(aprop_p, &aprop);
  rc = aprop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", aprop_p->rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_read_internal_common(int fd, void *buf, size_t count, 
  uint64_t requestId, void **bufPtr) {
  std::cout << "Inside " << __func__ << std::endl; 
#ifdef LDB_PRINT_CALL
  print_read(fd, buf, count, requestId);
#endif
  int wid = -1;
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READ);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  // ssize_t rc = fs_allocated_read_internal(service, fd, buf, count);
  auto offset = service->getOffset(fd);
  ssize_t rc = fs_allocated_pread_internal(service, fd, buf, count, offset, requestId, bufPtr);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  } else {
    service->updateOffset(fd, offset + rc - 1);
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READ, tsIdx);
#endif
  return rc;
}

ssize_t fs_allocated_read_retry(int fd, void *buf, size_t count, uint64_t requestId, void **bufPtr) {
  return fs_allocated_read_internal_common(fd, buf, count, requestId, bufPtr);
}

ssize_t fs_allocated_read(int fd, void *buf, size_t count, void **bufPtr) {
  std::cout << "Inside " << __func__ << std::endl; 
  auto requestId = getNewRequestId();
  return fs_allocated_read_internal_common(fd, buf, count, requestId, bufPtr);
}

ssize_t fs_allocated_pread_internal_common(int fd, void *buf, size_t count, 
  off_t offset, uint64_t requestId, void **bufPtr) {
  std::cout << "Inside " << __func__ << std::endl; 
#ifdef LDB_PRINT_CALL
  print_pread(fd, buf, count, offset, requestId);
#endif
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    LeaseUnref(entry);
    fd = base_fd;
  }
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pread_internal(service, fd, buf, count, offset, requestId, bufPtr);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  } 
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif
  return rc;
}

ssize_t fs_allocated_pread_retry(int fd, void *buf, size_t count, off_t offset, 
  uint64_t requestId, void **bufPtr) {
  return fs_allocated_pread_internal_common(fd, buf, count, offset, requestId, bufPtr);
}

ssize_t fs_allocated_pread(int fd, void *buf, size_t count, off_t offset, void **bufPtr) {
  auto requestId = getNewRequestId();
  return fs_allocated_pread_internal_common(fd, buf, count, offset, requestId, bufPtr);
}

// Note: not being used
static ssize_t fs_allocated_write_internal(FsService *fsServ, int fd, void *buf,
                                           size_t count) {
  struct shmipc_msg msg;
  struct allocatedWriteOpPacked *awop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedWriteOp awop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, true, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_write_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  awop_p = (struct allocatedWriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                        ring_idx);
  prepare_allocatedWriteOp(&msg, awop_p, fd, count);
  awop_p->alOp.shmid = shmid;
  awop_p->alOp.dataPtrId = dataPtrId;

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    rc = fs_retry_pending_ops();
  }
  unpack_allocatedWriteOp(awop_p, &awop);
  rc = awop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "flag:%u seqNo:%lu\n", awop.rwOp.flag, awop.alOp.perAppSeqNo);
  fprintf(stderr, "returned offset:%ld\n", awop.rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

static ssize_t fs_allocated_pwrite_internal(FsService *fsServ, int fd,
                                            void *buf, size_t count,
                                            off_t offset, uint64_t requestId) {
  struct shmipc_msg msg;
  struct allocatedPwriteOpPacked *apwop_p;
  off_t ring_idx;
  ssize_t rc;
  fslib_malloc_block_cnt_t dataPtrId;
  uint8_t shmid;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "first char:%c firstDataPtr:%c (dataPtr - shmStartPtr):%lu\n",
          *(static_cast<char *>(buf)),
          *(static_cast<char *>(threadMemBuf->firstDataPtr())),
          (static_cast<char *>(buf) -
           (reinterpret_cast<char *>(threadMemBuf->firstMetaPtr()))));
#endif

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, true, shmid, dataPtrId, err);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "shmId:%u dataaPtrId:%u\n", shmid, dataPtrId);
#endif
  if (err) {
    fprintf(stderr, "fs_allocated_pwrite_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  apwop_p = (struct allocatedPwriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                          ring_idx);
  prepare_allocatedPwriteOp(&msg, apwop_p, fd, count, offset, requestId);
  apwop_p->alOp.shmid = shmid;
  apwop_p->alOp.dataPtrId = dataPtrId;

  if (gServMngPtr->reqRingMap.count(requestId) == 0) {
    auto pendingOpIdx = gServMngPtr->queueMgr->enqueuePendingMsg(&msg);
    gServMngPtr->queueMgr->enqueuePendingXreq<struct allocatedPwriteOpPacked>(apwop_p, pendingOpIdx);
    gServMngPtr->reqRingMap[requestId].push_back(pendingOpIdx);
    gServMngPtr->reqAllocatedDataMap[requestId] = (char *)buf;
  }

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    threadFsTid = 0;
    return fs_retry_pending_ops();  
  }
  rc = apwop_p->rwOp.ret;

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_write_internal_common(int fd, void *buf, size_t count, 
  uint64_t requestId) {
#ifdef LDB_PRINT_CALL
  print_write(fd, buf, count, requestId);
#endif
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);

    ssize_t rc = -1;
    {
      std::unique_lock<std::shared_mutex> guard(entry->lock);
      int fd_offset = OpenLease::FindOffset(fd);
      LocalFileObj &file_object = entry->lease->GetFileObjects()[fd_offset];
      int offset = file_object.offset;

      rc = fs_allocated_pwrite(base_fd, buf, count, offset);
      if (rc >= 0) {
        size_t current_size = offset + rc;
        if (current_size > entry->lease->size)
          entry->lease->size = current_size;
        file_object.offset += rc;
      }
    }
    LeaseUnref(entry);
    return rc;
  }

#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_WRITE);
#endif
  int wid = -1;
retry:
  auto service = getFsServiceForFD(fd, wid);
  auto offset = service->getOffset(fd);
  // ssize_t rc = fs_allocated_write_internal(service, fd, buf, count);
  ssize_t rc = fs_allocated_pwrite_internal(service, fd, buf, count, offset, requestId);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_allocated_write fd:%d count:%ld rc:%ld\n", fd, count, rc);
#endif
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  } else {
    service->updateOffset(fd, offset + rc - 1);
  }

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_WRITE, tsIdx);
#endif
  return rc;
}

ssize_t fs_allocated_write_retry(int fd, void *buf, size_t count, 
  uint64_t requestId) {
  return fs_allocated_write_internal_common(fd, buf, count, requestId);
}

ssize_t fs_allocated_write(int fd, void *buf, size_t count) {
  auto requestId = getNewRequestId();
  return fs_allocated_write_internal_common(fd, buf, count, requestId);
}


ssize_t fs_allocated_pwrite_internal_common(int fd, void *buf, ssize_t count, 
  off_t offset, uint64_t requestId) {
  int wid = -1;
#ifdef LDB_PRINT_CALL
  print_pwrite(fd, buf, count, offset, requestId);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PWRITE);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pwrite_internal(service, fd, buf, count, offset, requestId);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PWRITE, tsIdx);
#endif
  return rc;
}

ssize_t fs_allocated_pwrite_retry(int fd, void *buf, ssize_t count, 
  off_t offset, uint64_t requestId) {
  return fs_allocated_pwrite_internal_common(fd, buf, count, offset, requestId);
}

ssize_t fs_allocated_pwrite(int fd, void *buf, ssize_t count, 
  off_t offset) {
  auto requestId = getNewRequestId();
  return fs_allocated_pwrite_internal_common(fd, buf, count, offset, requestId);
}

#if 0
// used for rbtree
static int compare_file_offset(node n, void *leftp, void *rightp) {
  off_t left = (off_t)leftp;
  off_t right = (off_t)rightp;
  if (left < right)
    return -1;
  else if (left > right)
    return 1;
  else {
    assert(left == right);
    return 0;
  }
}
#endif
/* #endregion */

/* #region fs cache*/
constexpr static int gRWLookupLocalErr_FdNotFound = -11;
constexpr static int gRWLookupLocalErr_NoData = -13;
constexpr static int gRWLookupLocalSuccess = 0;

static int fromFdToFileHandle(int fd, struct FileHandle **fhPtr) {
  gLibSharedContext->fdFileHandleMapLock.lock_read();
  auto fhIt = gLibSharedContext->fdFileHandleMap.find(fd);

  if (fhIt == gLibSharedContext->fdFileHandleMap.end()) {
    fprintf(stderr, "ERROR cannot find this fd's corresponding fileHandle\n");
    gLibSharedContext->fdFileHandleMapLock.unlock();
    return gRWLookupLocalErr_FdNotFound;
  }

  *fhPtr = fhIt->second;
  gLibSharedContext->fdFileHandleMapLock.unlock();
  return 0;
}

// lookup continuous file content in client cache
// REQUIRED: startOff is aligned to page size
// @param blkRcdVec: result will be saved to this array
//   - REQUIRED: len(buf_arr) == numPages
// @return 0 if success, or above error numbers
static int rwLookupLocalCache(
    struct FileHandle *fh, std::vector<struct BlockRecordOfFile *> &blkRcdVec,
    off_t startOff, int numPages) {
  assert(startOff % gFsLibMallocPageSize == 0);
  assert(blkRcdVec.size() == (uint)numPages);

  std::vector<struct BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fh->id];

  uint32_t max_offset = startOff / gFsLibMallocPageSize + numPages;
  if (fhPageMap.size() < max_offset + 1) {
    fhPageMap.resize(max_offset + 1, nullptr);
  }
  for (int i = 0; i < numPages; i++) {
    uint32_t block_offset = startOff / gFsLibMallocPageSize + i;
    BlockRecordOfFile *blk = fhPageMap[block_offset];
    if (blk == nullptr) {
      return gRWLookupLocalErr_NoData;
    } else {
      blkRcdVec[i] = blk;
    }
  }
  return gRWLookupLocalSuccess;
}

static int rwSaveDataToCache(struct FileHandle *fh, off_t alignedOff,
                             int numPages, void *buf,
                             FsLeaseCommon::rdtscmp_ts_t ts, bool isWrite) {
  if (gIsCCLeaseDbg)
    fprintf(stderr,
            "rwSaveDataToCache fh:%p alignedOff:%ld numPages:%d buf:%p\n", fh,
            alignedOff, numPages, buf);
  std::vector<struct BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fh->id];

  off_t curAlignedOff;
  uint32_t max_offset = numPages + alignedOff / gFsLibMallocPageSize;
  if (fhPageMap.size() < max_offset + 1) {
    fhPageMap.resize(max_offset + 1, nullptr);
  }
  for (int i = 0; i < numPages; i++) {
    uint32_t block_offset = alignedOff / gFsLibMallocPageSize + i;
    curAlignedOff = alignedOff + i * gFsLibMallocPageSize;
    BlockRecordOfFile *&blk = fhPageMap[block_offset];
    if (blk != nullptr) {
      fprintf(stderr, "ERROR curAlignedOffset:%ld is found in the cache\n",
              curAlignedOff);
      throw std::runtime_error("curAlignedOff cannot find in the cache");
    }
    auto blkRcdPtr = new BlockRecordOfFile();
    blkRcdPtr->alignedStartOffset = curAlignedOff;
    blkRcdPtr->addr = static_cast<char *>(buf) + i * gFsLibMallocPageSize;
    if (isWrite) blkRcdPtr->flag |= FSLIB_APP_BLK_RCD_FLAG_DIRTY;
    blk = blkRcdPtr;
  }
  return 0;
}

// @param count: size in bytes
static inline void computeAlignedPageNum(off_t startOffset, size_t count,
                                         off_t &alignedOffset, uint &numPages) {
  alignedOffset = (startOffset / gFsLibMallocPageSize) * gFsLibMallocPageSize;
  off_t endOffset = startOffset + count;
  numPages = 1 + (endOffset - alignedOffset - 1) / gFsLibMallocPageSize;
}

// @param offset: offset of this read/write.
//   if offset == kfsRwRenewLeaseOffsetRenewOnly, then it is a pure renew op,
//   will not put *buf*, *count*, *offset*, into the *packed_op*
//   REQUIRED: in this case, buf should be set to nullptr, count arg as 1
// @param ts: if set to > 0, then it indicates that lease granted
// @return return of the R/W data operation
//   NOTE: this must be set either for (p)read/(p)write, since once cache is
//   used, it might be that offset maintenance is not part of the FSP server job
//   I.e., whether App itself calls cached_read() or cached_pread(), FSP will
//   only regard it as pread()
constexpr static off_t kfsRwRenewLeaseOffsetRenewOnly = -1;
template <class T1, class T2>
static ssize_t fs_rw_renew_lease(FsService *fsServ, int fd, void *buf,
                                 void *buf_head, size_t count, off_t offset,
                                 FsLeaseCommon::rdtscmp_ts_t &ts,
                                 void (*set_lease_flag_func)(T2 *),
                                 void (*prep_op_fun)(struct shmipc_msg *, T2 *,
                                                     int, size_t, off_t, uint64_t),
                                 void (*unpack_func)(T2 *, T1 *)) {
  T2 *top_packed;

  struct shmipc_msg msg;
  off_t ring_idx;
  ssize_t rc;

  if (gIsCCLeaseDbg)
    fprintf(stderr, "fs_rw_new_lease: fd:%d buf:%p count:%lu offset:%ld\n", fd,
            buf, count, offset);

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  top_packed =
      reinterpret_cast<T2 *>(IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx));
  prep_op_fun(&msg, top_packed, fd, count, offset, 0);

  set_lease_flag_func(top_packed);

  if (offset != kfsRwRenewLeaseOffsetRenewOnly) {
    assert(buf != nullptr);
    int err = 0;
    threadMemBuf->getBufOwnerInfo(buf_head, false, top_packed->alOp.shmid,
                                  top_packed->alOp.dataPtrId, err);
    long diff = (char *)(buf) - (char *)(buf_head);
    assert(diff >= 0);
    if (diff > 0) {
      top_packed->rwOp.realCount = diff;
    } else {
      top_packed->rwOp.realCount = 0;
    }
    if (err) {
      fprintf(stderr, "fs_rw_renew_lease: Error in getBufOwnerInfo\n");
    } else {
#ifdef _CFS_LIB_DEBUG_
      dumpAllocatedOpCommon(&top_packed->alOp);
#endif
    }
  } else {
    setLeaseOpRenewOnly(top_packed);
    // NOTE: if buf == nullptr, it is a renew only request
    assert(count == 0);
    assert(buf == nullptr);
  }

  // send request
  // TODO: Not sure if exponential backoff should be used here
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  T1 top;
  unpack_func(top_packed, &top);
  rc = top_packed->rwOp.ret;

  bool isWidUpdated = checkUpdateFdWid(static_cast<int>(rc), fd);

  if (!isWidUpdated) {
    if (isLeaseHeld(&top.rwOp)) {
      ts = getLeaseTermTsFromRwOp(&top.rwOp);
      if (gIsCCLeaseDbg) fprintf(stderr, "lease held ts is set to:%lu\n", ts);
    } else {
      // TODO (jingliu): what should we do here if lease cannot be granted?
      // Option-1: Let return the corresponding error to the caller
      // Option-2: We fall back to fs_allocated_read()?
      if (gIsCCLeaseDbg)
        fprintf(stderr, "lease not held, set ret to:%d\n",
                (FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE));
      ts = 0;
      rc = FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE;
    }
  }

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);

  return rc;
}

// NOTE: if offset < 0, we think it comes from fs_cached_read(), and will
// try to find the offset in *FsLib*
ssize_t fs_cached_pread(int fd, void **bufAddr, size_t count, off_t offset) {
  assert((*bufAddr) == nullptr);
  off_t curOffset = offset;
  // this buf is supposed to be aligned to page size and is exactly the starting
  // byte addr
  // void *buf = *(bufAddr);
  void *buf = nullptr;
  if (offset < 0) {
    // this comes from a read() request, set the offset via saved offset
    auto it = gLibSharedContext->fdCurOffMap.find(fd);
    assert(it != gLibSharedContext->fdCurOffMap.end());
    curOffset = it->second;
    if (gIsCCLeaseDbg)
      fprintf(
          stderr,
          "fs_cached_pread() count:%ld offset:%ld curOffset is set to:%ld\n",
          count, offset, curOffset);
  }

  off_t alignedOffset;
  uint numPages;
  computeAlignedPageNum(curOffset, count, alignedOffset, numPages);
  if (gIsCCLeaseDbg)
    fprintf(stderr, "alignedOffset:%ld numPages:%u\n", alignedOffset, numPages);

  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  if (widIt == gLibSharedContext->fdWidMap.end()) {
    return -1;
  }

  struct FileHandle *fhPtr = nullptr;
  int rc = fromFdToFileHandle(fd, &fhPtr);
  if (rc == gRWLookupLocalErr_FdNotFound) {
    // if fd can not be found, there is something seriously wrong
    throw std::runtime_error("ccache lookup. cannot find fd\n");
  }

  // try to lookup in local cache
  auto blkRcdVec = std::vector<struct BlockRecordOfFile *>(numPages);
  rc = rwLookupLocalCache(fhPtr, blkRcdVec, alignedOffset, numPages);
  int retnr = -1;
  switch (rc) {
    case gRWLookupLocalErr_NoData:
      // ccache miss, need to fetch the data from server
      FsLeaseCommon::rdtscmp_ts_t ts;
      buf = fs_malloc(count);
      if (gIsCCLeaseDbg) {
        fprintf(stderr,
                "cache miss, need to fetch data from the server, allocated "
                "addr:%p count:%ld\n",
                buf, count);
      }
    CACHED_PREAD_RW_NODATA_WID_UPDATE_RETRY:
      retnr = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
          gServMngPtr->multiFsServMap[widIt->second], fd, buf, buf,
          numPages * gFsLibMallocPageSize, alignedOffset, ts, setLeaseOpForRead,
          prepare_allocatedPreadOp, unpack_allocatedPreadOp);
      if (gIsCCLeaseDbg)
        fprintf(
            stderr,
            "fs_rw_renew_lease get retnr:%d bufPtr:%p count:%lu firstChar:%c\n",
            retnr, buf, count, static_cast<char *>(buf)[0]);
      if (retnr == FS_REQ_ERROR_INODE_REDIRECT) {
        fprintf(stdout, "FS_REQ_ERROR_INODE_REDIRECT retry\n");
        widIt = gLibSharedContext->fdWidMap.find(fd);
        goto CACHED_PREAD_RW_NODATA_WID_UPDATE_RETRY;
      }
      if (retnr >= (int64_t)count) {
        rwSaveDataToCache(fhPtr, alignedOffset, numPages, buf, ts,
                          /*isWrite*/ false);
      }

      // retnr comes from the *fs_rw_renew_lease* which is page aligned in terms
      // of R/W uints
      if (retnr > (ssize_t)count) {
        retnr = count;
      }

      // set the valid data addrs
      if (retnr >= 0) {
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        *(bufAddr) = static_cast<char *>(buf) + (curOffset - alignedOffset);
      }
      break;
    case gRWLookupLocalSuccess:
      // ccache hit
      // first check the lease
      if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
        if (gIsCCLeaseDbg) fprintf(stderr, "LookupSucceed: lease is valid\n");
        // we can use ccache without contacting the server
        off_t offDiff = curOffset - alignedOffset;
        buf = static_cast<void *>((static_cast<char *>(blkRcdVec[0]->addr)));
        if (gIsCCLeaseDbg)
          fprintf(stderr, "buf addr:%p firstChar:%c\n", buf,
                  static_cast<char *>(buf)[0]);
        *(bufAddr) = static_cast<char *>(buf) + offDiff;
        retnr = count;
      } else {
        if (gIsCCLeaseDbg) {
          fprintf(stderr, "LookupSucceed: lease is not valid, need to renew\n");
        }
        // lease expired, need to renew the lease first
        FsLeaseCommon::rdtscmp_ts_t ts = 0;
      CACHED_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
        int lrnrt = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
            gServMngPtr->multiFsServMap[widIt->second], fd, /*buf*/ nullptr,
            nullptr,
            /*count*/ 0, kfsRwRenewLeaseOffsetRenewOnly, ts, setLeaseOpForRead,
            prepare_allocatedPreadOp, unpack_allocatedPreadOp);
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        if (lrnrt != 0) {
          if (gIsCCLeaseDbg) fprintf(stderr, "lease renew fail\n");
          if (lrnrt == FS_REQ_ERROR_INODE_REDIRECT) {
            fprintf(stdout, "FS_REQ_ERROR_INODE_REDIRECT retry\n");
            widIt = gLibSharedContext->fdWidMap.find(fd);
            goto CACHED_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
          }
          retnr = (FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE);
          // TODO (jingliu): once fail to extend lease, need to clean up local
          // cached data, and let App knows to fail-back to fs_allocated_r/w
        } else {
          // lease renew success
          off_t offDiff = curOffset - alignedOffset;
          buf = static_cast<void *>((static_cast<char *>(blkRcdVec[0]->addr)));
          if (gIsCCLeaseDbg)
            fprintf(stderr,
                    "retrieve data from ccache offDiff:%ldf firstChar:%c\n",
                    offDiff, static_cast<char *>(buf)[0]);
          *(bufAddr) = static_cast<char *>(buf) + offDiff;
          retnr = count;
        }
      }
      break;
    default:
      fprintf(stderr, "ERROR return of lookup is not supported\n");
  }
  return retnr;
}

ssize_t fs_cached_read(int fd, void **bufAddr, size_t count) {
  ssize_t rc = fs_cached_pread(fd, bufAddr, count, /*offset*/ -2);
  // update offset
  gLibSharedContext->fdCurOffMap[fd] += count;
  return rc;
}

ssize_t fs_cached_posix_pread(int fd, void *buf, size_t count, off_t offset) {
  void *tmpBuf = nullptr;
  ssize_t ret = fs_cached_pread(fd, &tmpBuf, count, offset);
  assert(ret > 0);
  if (gIsCCLeaseDbg) {
    fprintf(stderr, "fs_cached_pread return tmpBuf is set to:%p\n", tmpBuf);
  }
  memcpy(buf, tmpBuf, count);
  return ret;
}

static constexpr int k_cpc_page_size = 4096;

// #define CPC_DEBUG_PRINT
// #define CPC_HIT_RATIO_REPORT

#ifdef CPC_HIT_RATIO_REPORT
static constexpr int k_cpc_num_report_thr = 100000;
thread_local int cpc_num_hit = 0;
thread_local int cpc_num_op = 0;
#endif

ssize_t fs_cpc_pread(int fd, void *buf, size_t count, off_t offset) {
#ifdef CPC_DEBUG_PRINT
  fprintf(stderr, "fs_cpc_pread: fd:%d count:%lu off_t:%lu\n", fd, count,
          offset);
#endif
#ifdef CPC_HIT_RATIO_REPORT
  cpc_num_op++;
  if (cpc_num_op > k_cpc_num_report_thr) {
    fprintf(stderr, "tid:%d num_op_done:%d num_hit:%d hit_ratio:%f\n",
            threadFsTid, cpc_num_op, cpc_num_hit,
            float(cpc_num_hit) / (cpc_num_op));
    cpc_num_op = 0;
    cpc_num_hit = 0;
  }
#endif

  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    LeaseUnref(entry);
    fd = base_fd;
  }

  struct FileHandle *fhPtr = nullptr;
  fromFdToFileHandle(fd, &fhPtr);
  assert(fhPtr != nullptr);
  if (count == 0) return 0;
  // lookup in the local page cache
  // std::unordered_map<off_t, BlockRecordOfFile *> &fhPageMap =
  //     gLibSharedContext->fhIdPageMap[fhPtr->id];
  std::vector<BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fhPtr->id];

  off_t aligned_start_off = (offset / k_cpc_page_size) * k_cpc_page_size;
  size_t count_from_aligned_off = count;
  if (aligned_start_off < offset)
    count_from_aligned_off += (offset - aligned_start_off);

  uint64_t off = offset, tot;
  uint32_t m;
  char *dst = (char *)buf;
  bool success = true;
  fhPtr->lock_.lock_read();
  uint32_t page_offset = (count + offset) / k_cpc_page_size;
  if (fhPageMap.size() < page_offset + 1) {
    fhPageMap.resize(page_offset + 1, nullptr);
  } else {
    for (tot = 0; tot < count; tot += m, off += m, dst += m) {
      uint32_t block_idx = off / k_cpc_page_size;
      m = std::min(count - tot, k_cpc_page_size - off % k_cpc_page_size);
      auto block = fhPageMap[block_idx];
      if (block == nullptr) {
        success = false;
        break;
      }
      // if any page except the last one is not full, the lookup fail
      if (block->count < k_cpc_page_size && m + tot != count) {
        success = false;
        break;
      }
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr,
              "find: off:%lu aligned_off:%lu count:%lu count- tot:%lu\n", off,
              cur_aligned_off, it->second->count, it->second->count - tot);
#endif
      memcpy(dst, (char *)(block->addr) + off % k_cpc_page_size, m);
    }
  }
  fhPtr->lock_.unlock();
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  assert(widIt != gLibSharedContext->fdWidMap.end());
  int retnr = -1;
  if (success && tot == count) {
    // see if lease expire
    if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "cache hit fd:%d off:%ld\n", fd, offset);
#endif
#ifdef CPC_HIT_RATIO_REPORT
      cpc_num_hit++;
#endif
      return count;
    } else {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "renew only fd:%d off:%ld\n", fd, offset);
#endif
      // renew lease
    CPC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
      FsLeaseCommon::rdtscmp_ts_t ts = 0;
      int lrnrt = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
          gServMngPtr->multiFsServMap[widIt->second], fd, /*buf*/ nullptr,
          nullptr,
          /*count*/ 0, kfsRwRenewLeaseOffsetRenewOnly, ts, setLeaseOpForRead,
          prepare_allocatedPreadOp, unpack_allocatedPreadOp);
      gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
      if (lrnrt != 0) {
        // lease renew fail
        if (lrnrt == FS_REQ_ERROR_INODE_REDIRECT) {
          widIt = gLibSharedContext->fdWidMap.find(fd);
          goto CPC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
        }
        // TODO (jingliu)
        throw std::runtime_error("lease renew fail");
      } else {
        // lease renew succeed
        return count;
      }
    }
  } else {
    // fetch data from the server
#ifdef CPC_DEBUG_PRINT
    fprintf(stderr, "fetch data from the server\n");
#endif
    FsLeaseCommon::rdtscmp_ts_t ts;
  CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY:
    retnr = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
        gServMngPtr->multiFsServMap[widIt->second], fd,
        (char *)buf - (offset - aligned_start_off),
        (char *)buf - k_cpc_page_size, count_from_aligned_off,
        aligned_start_off, ts, setLeaseOpForRead, prepare_allocatedPreadOp,
        unpack_allocatedPreadOp);
    if (retnr > 0) {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "save data to cache: fd:%d off:%ld\n", fd, offset);
#endif
      // save the data to local page cache
      uint64_t off = aligned_start_off, tot;
      uint32_t m;
      fhPtr->lock_.lock();
      gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
      char *src = (char *)buf - (offset - aligned_start_off);
      for (tot = 0; tot < count_from_aligned_off;
           tot += m, off += m, src += m) {
        uint32_t block_idx = off / k_cpc_page_size;
        assert(off % k_cpc_page_size == 0);
        m = std::min(count_from_aligned_off - tot,
                     k_cpc_page_size - off % k_cpc_page_size);
        // uint64_t cur_aligned_off = (uint64_t)block_idx * k_cpc_page_size;
        // BlockRecordOfFile *&blk = fhPageMap[cur_aligned_off];
        BlockRecordOfFile *&blk = fhPageMap[block_idx];
        if (blk == nullptr) {
          auto cur_blk = new BlockRecordOfFile();
          blk = cur_blk;
          blk->alignedStartOffset = off;
          blk->addr = scalable_aligned_malloc(k_cpc_page_size, k_cpc_page_size);
          // blk->addr = malloc(k_cpc_page_size);
          // assert(blk->addr != nullptr);
          if (blk->addr == nullptr) {
            fprintf(stderr, "cannot allocate memory\n");
            fhPtr->lock_.unlock();
            goto CPC_ERROR;
          }
        }
        if (m > blk->count) blk->count = m;
#ifdef CPC_DEBUG_PRINT
        fprintf(stderr, "--off:%ld m:%u count:%ld\n", off, m, blk->count);
#endif
        memcpy(blk->addr, src, m);
      }
      fhPtr->lock_.unlock();
      return retnr - (offset - aligned_start_off);
    } else if (retnr == 0) {
      return 0;
    } else {
      // printf("cpc retval: %d\n", retnr);
      if (handle_inode_in_transfer(retnr))
        goto CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY;
      if (checkUpdateFdWid(retnr, fd))
        goto CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY;
    }
  }
CPC_ERROR:
  return -1;
}

ssize_t fs_uc_read(int fd, void *buf, size_t count) {
  auto it = gLibSharedContext->fdOffsetMap.find(fd);
  assert(it != gLibSharedContext->fdOffsetMap.end());
  off_t off = it->second;
  ssize_t nread = fs_uc_pread(fd, buf, count, off);
  if (nread >= 0) {
    gLibSharedContext->fdOffsetMap[fd] = off + nread;
  }
  return nread;
}

// static const uint64_t kUcOpByteMax =
//     ((RING_DATA_ITEM_SIZE) / (CACHE_LINE_BYTE)) * (PAGE_CACHE_PAGE_SIZE);

void updateRcTsForLease(rwOpCommon *op, FsLeaseCommon::rdtscmp_ts_t &ts,
                        ssize_t &rc) {
  if (isLeaseHeld(op)) {
    ts = getLeaseTermTsFromRwOp(op);
  } else {
    ts = 0;
    rc = FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE;
  }
}

ssize_t fs_uc_pread_renewonly_internal(FsService *fsServ, int fd,
                                       FsLeaseCommon::rdtscmp_ts_t &ts) {
  struct shmipc_msg msg;
  struct preadOpPacked *prop_p;
  struct preadOp prop;
  ssize_t rc;
  off_t ring_idx;
  memset(&msg, 0, sizeof(msg));

  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_preadOpForUC(&msg, prop_p, fd, 0, 0);
  setLeaseOpRenewOnly(prop_p);
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    rc = fs_retry_pending_ops();
  }
  unpack_preadOp(prop_p, &prop);
  rc = prop.rwOp.ret;

  updateRcTsForLease(&prop.rwOp, ts, rc);

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_uc_pread_internal(FsService *fsServ, int fd, void *buf,
                             size_t orig_count, off_t orig_off,
                             size_t count_from_aligned, off_t offset_aligned,
                             FileHandle *fh, FsLeaseCommon::rdtscmp_ts_t &ts) {
  // fprintf(stdout, "fs_uc_pread_internal\n");
  // assert(count_from_aligned < kUcOpByteMax);
  struct shmipc_msg msg;
  struct preadOpPacked *prop_p;
  struct preadOp prop;
  ssize_t rc;
  off_t ring_idx;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  prepare_preadOpForUC(&msg, prop_p, fd, count_from_aligned, offset_aligned);
  setLeaseOpForReadUC<preadOpPacked>(prop_p);

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    rc = fs_retry_pending_ops();
  }

  unpack_preadOp(prop_p, &prop);
  rc = prop.rwOp.ret;
  if (rc > 0) {
    // record pages to cacheHelper
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    PageDescriptorMsg *pdmsg =
        reinterpret_cast<PageDescriptorMsg *>(curDataPtr);
    PageDescriptorMsg *curmsg;
    uint64_t off = orig_off, tot;
    uint32_t m;
    fh->lock_.lock();
    char *dstPtr = static_cast<char *>(buf);
    int pdmsgIdx = 0;
    for (tot = 0; tot < orig_count;
         tot += m, off += m, dstPtr += m, pdmsgIdx++) {
      uint32_t pageIdx = off / (PAGE_CACHE_PAGE_SIZE);
      curmsg = pdmsg + pdmsgIdx;
      m = std::min(orig_count - tot,
                   (PAGE_CACHE_PAGE_SIZE)-off % (PAGE_CACHE_PAGE_SIZE));
      assert(checkPdMsgMAGIC(curmsg));
      assert(curmsg->pageIdxWithinIno == pageIdx);
      // fprintf(stdout, "off:%lu curmsg->gPageIdx:%u\n", off,
      // curmsg->gPageIdx);
      auto curpair =
          gLibSharedContext->pageCacheHelper.findStablePage(curmsg->gPageIdx);
      // copy the data to destination
      memcpy(dstPtr,
             static_cast<char *>(curpair.second) + off % (PAGE_CACHE_PAGE_SIZE),
             m);
      // install the cache pages
      fh->cacheReader.installCachePage(pageIdx * (PAGE_CACHE_PAGE_SIZE),
                                       curpair);
    }
    fh->lock_.unlock();
    rc = rc - (orig_off - offset_aligned);
  }
  // extract ts
  updateRcTsForLease(&prop.rwOp, ts, rc);

  // process returned list of pages
  // now it's safe to dealloc this shm slot
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_uc_pread(int fd, void *buf, size_t count, off_t offset) {
  struct FileHandle *fhPtr = nullptr;
  fromFdToFileHandle(fd, &fhPtr);
  assert(fhPtr != nullptr);
  if (count == 0) return 0;
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_uc_pread count:%lu offset:%lu\n", count, offset);
#endif
  // lookup page cache
  off_t aligned_start_off =
      (offset / PAGE_CACHE_PAGE_SIZE) * (PAGE_CACHE_PAGE_SIZE);
  size_t count_from_aligned_off = count;
  if (aligned_start_off < offset)
    count_from_aligned_off += (offset - aligned_start_off);
  fhPtr->lock_.lock_read();
  bool success = fhPtr->cacheReader.lookupRange(offset, count, buf);
  fhPtr->lock_.unlock();
  // deal with lease
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  assert(widIt != gLibSharedContext->fdWidMap.end());
  int retnr = -1;
  int wid = -1;
  if (success) {
    if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
      // fprintf(stdout, "cacheHit\n");
      return count;
    } else {
      // fprintf(stdout, "only need to renew\n");
      // renew lease
      FsLeaseCommon::rdtscmp_ts_t ts = 0;
    UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
      auto service = getFsServiceForFD(fd, wid);
      int lrnrt = fs_uc_pread_renewonly_internal(service, fd, ts);
      if (handle_inode_in_transfer(lrnrt))
        goto UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
      bool should_retry = checkUpdateFdWid(static_cast<int>(lrnrt), fd);
      if (should_retry) goto UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
      if (lrnrt < 0) {
        throw std::runtime_error("cannot renew lease");
      } else {
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        return count;
      }
    }
  } else {
    // fprintf(stdout, "fetch data from server\n");
    // fetch data from the server
    FsLeaseCommon::rdtscmp_ts_t ts = 0;
  UC_PREAD_DATA_FETCH_RETRY:
    auto service = getFsServiceForFD(fd, wid);
    retnr = fs_uc_pread_internal(service, fd, buf, count, offset,
                                 count_from_aligned_off, aligned_start_off,
                                 fhPtr, ts);
    if (handle_inode_in_transfer(retnr)) goto UC_PREAD_DATA_FETCH_RETRY;
    bool should_retry = checkUpdateFdWid(retnr, fd);
    if (should_retry) goto UC_PREAD_DATA_FETCH_RETRY;
    gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
    return retnr;
  }
  return 0;
}

/* #endregion */

off_t fs_lseek_internal(FsService *fsServ, int fd, long int offset,
                        int whence, off_t file_offset) {
  struct shmipc_msg msg;
  struct lseekOp *op;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  op = (struct lseekOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_lseekOp(&msg, op, fd, offset, whence, file_offset);
  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
  }

  ret = op->ret;
  fsServ->setOffset(fd, ret);

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_lseek(fd:%d) ret:%d\n", fd, ret);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// TODO: Add pending
off_t fs_lseek(int fd, long int offset, int whence) {
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    int fd_offset = OpenLease::FindOffset(fd);
    {
      std::unique_lock<std::shared_mutex> guard(entry->lock);
      LocalFileObj &file_object = entry->lease->GetFileObjects()[fd_offset];
      struct stat *stat_buffer = nullptr;
      int stat_ret = -1;
      switch (whence) {
        case SEEK_SET:
          file_object.offset = offset;
          break;
        case SEEK_CUR:
          file_object.offset += offset;
          break;
        case SEEK_END:
          stat_buffer = new struct stat;
          stat_ret = fs_stat(entry->lease->GetPath().c_str(), stat_buffer);
          assert(stat_ret == 0);
          entry->lease->size = stat_buffer->st_size;
          file_object.offset = entry->lease->size + offset;
          delete stat_buffer;
          break;
        default:
          // not supporting SEEK_DATA & SEEK_HOLE
          assert(false);
      }
    }
    LeaseUnref(entry);
    return 0;
  }

retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  auto file_offset = service->getOffset(fd);
  int rc = fs_lseek_internal(service, fd, offset, whence, file_offset);

  if (rc >= 0) {
    service->updateOffset(fd, rc - 1);
    return rc;
  }
  if (handle_inode_in_transfer(rc)) goto retry;

  bool should_retry = checkUpdateFdWid(rc, fd);
  if (should_retry) goto retry;
  return rc;
}


#ifdef FS_LIB_SPPG
void init_global_spdk_env() {
  gSpdkEnvPtr.reset(new SpdkEnvWrapper());
  spdk_env_opts_init(&gSpdkEnvPtr->opts);
  gSpdkEnvPtr->opts.name = "spdkclient";
  gSpdkEnvPtr->opts.shm_id = SPDK_HUGEPAGE_GLOBAL_SHMID;
  if (spdk_env_init(&(gSpdkEnvPtr->opts)) < 0) {
    fprintf(stderr, "cannot init client pinned mem\n");
    exit(1);
  }
}

void check_init_mem() {
  assert(gServMngPtr != nullptr);
  std::call_once(gSpdkEnvFlag, init_global_spdk_env);
  if (!tlPinnedMemPtr) {
    tlPinnedMemPtr.reset(new FsLibPinnedMemMng());
    tlPinnedMemPtr->memPtr = spdk_dma_zmalloc(kLocalPinnedMemSize, 4096, NULL);
  }
}

ssize_t fs_sppg_cpc_pread(int fd, void *buf, size_t count, off_t offset) {
  check_init_mem();
  fprintf(stderr, "memPtr:%p\n", tlPinnedMemPtr->memPtr);
  return 0;
}
#endif

void *fs_malloc_pad(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Malloc(size + k_cpc_page_size, err);
  // fprintf(stderr, "fs_malloc_pad sz:%ld ret:%p ret_pad:%p\n", size, ptr,
  // (void*)((char*)ptr + k_cpc_page_size));
  return (char *)ptr + k_cpc_page_size;
}

void fs_free_pad(void *ptr, int fsTid) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready(fsTid);
  // fprintf(stderr, "fs_free_pad ptr:%p ptr_orig:%p\n", ptr, (char*)ptr -
  // k_cpc_page_size);
  threadMemBuf->Free((char *)ptr - k_cpc_page_size, err);
}

void fs_free_pad(void *ptr) { fs_free_pad(ptr, threadFsTid); }

void fs_init_thread_local_mem() { check_app_thread_mem_buf_ready(); }

void *fs_malloc(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Malloc(size + k_cpc_page_size, err);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "malloc_threadMemBuf %p tid:%d return ptr:%p\n", threadMemBuf,
          threadFsTid, ptr);
#endif
  // fprintf(stderr, "fs_malloc: return:%p tid:%d\n", ptr, threadFsTid);
  return ptr;
}

void *fs_zalloc(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Zalloc(size, err);
  return ptr;
}

void fs_free(void *ptr, int fsTid) {
  int err = 0;
  auto threadMemBuf = check_app_thread_mem_buf_ready(fsTid);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "free_threadMemBuf %p tid:%d ptr:%p firstDataPtr:%p\n",
          threadMemBuf, fsTid, ptr, threadMemBuf->firstDataPtr());
#endif
  // fprintf(stderr, "fs_free:%p tid:%d\n", ptr, threadFsTid);
  threadMemBuf->Free(ptr, err);
  if (err) fprintf(stderr, "free error: %d\n", err);
}

void fs_free(void *ptr) { fs_free(ptr, threadFsTid); }

/* #region fs ldb specific */
int fs_open_ldb(const char *path, int flags, mode_t mode) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPEN);
#endif
#ifdef LDB_PRINT_CALL
  print_open(path, flags, mode, true);
#endif
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // NOTE: the first time invoking this is going to be extremely costly, ~0.2s
  // So, it is important for any user (thread) to invoke this
  //(void) check_app_thread_mem_buf_ready();
  assert(threadFsTid != 0);
  bool isOpRetried = false;
  auto requestId = getNewRequestId();

retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  size_t file_size;
  int rc = fs_open_internal(service, standardPath, flags, mode, requestId, &file_size);

  if (rc > 0) {
    gLibSharedContext->fdWidMap[rc] = wid;
    goto free_and_return;
  } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
    goto free_and_return;
  }

  // migration related errors
  if (handle_inode_in_transfer(rc)) {
    isOpRetried = true;
    goto retry;
  }

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    isOpRetried = true;
    goto retry;
  }

free_and_return:
  if (rc > 0) {
    LDBLease *lease = nullptr;    std::string path_string(standardPath);
    auto it = gLibSharedContext->pathLDBLeaseMap.find(path_string);
    if (it == gLibSharedContext->pathLDBLeaseMap.end()) {
      lease = new LDBLease(file_size);
      gLibSharedContext->pathLDBLeaseMap.emplace(path_string, lease);
    } else {
      lease = it->second;
    }
    gLibSharedContext->fdLDBLeaseMap.emplace(rc, lease);
  }

  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPEN, tsIdx);
#endif
  return rc;
}

int fs_open_ldb2(const char *path, int flags) {
  return fs_open_ldb(path, flags, 0);
}

static ssize_t fs_allocated_pread_internal_ldb(FsService *fsServ, int fd,
                                               void *buf, size_t count,
                                               off_t offset,
                                               uint32_t mem_offset) {
  struct shmipc_msg msg;
  struct allocatedPreadOpPacked *aprop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedPreadOp aprop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo((char *)buf - gFsLibMallocPageSize, false,
                                shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_pread_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  } else {
#ifdef _CFS_LIB_DEBUG_
    dumpAllocatedOpCommon(&aprop_p->alOp);
#endif
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  aprop_p = (struct allocatedPreadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                         ring_idx);
  prepare_allocatedPreadOp(&msg, aprop_p, fd, count, offset);
  aprop_p->alOp.shmid = shmid;
  aprop_p->alOp.dataPtrId = dataPtrId;

  // reset sequential number
  aprop_p->alOp.perAppSeqNo = 0;
  aprop_p->rwOp.realCount = mem_offset;

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    rc = fs_retry_pending_ops();
  }
  unpack_allocatedPreadOp(aprop_p, &aprop);
  rc = aprop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", aprop_p->rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_pread_ldb(int fd, void *buf, size_t count, off_t offset) {
#ifdef LDB_PRINT_CALL
  print_pread(fd, buf, count, offset, true);
#endif

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by pread %d\n", fd);
    throw std::runtime_error("LDBLease not found by pread");
  }

  LDBLease *lease = it->second;
  size_t new_count = 0;
  off_t new_offset = 0;
  int num_hit = 0;
  bool success =
      lease->Read(buf, count, offset, &new_count, &new_offset, &num_hit);
  if (success) {
    return count;
  }
  uint32_t mem_offset = gFsLibMallocPageSize - offset % gFsLibMallocPageSize +
                        gFsLibMallocPageSize * num_hit;

  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pread_internal_ldb(service, fd, buf, new_count,
                                               new_offset, mem_offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }

  lease->Write((char *)buf - gFsLibMallocPageSize + mem_offset, new_count,
               new_offset, threadFsTid);

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif
  return count;
}

ssize_t fs_allocated_write_ldb(int fd, void *buf, size_t count) {
#ifdef LDB_PRINT_CALL
  print_write(fd, buf, count, true);
#endif

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by write %d\n", fd);
    throw std::runtime_error("LDBLease not found by write");
  }

  LDBLease *lease = it->second;
  lease->Write(buf, count, threadFsTid);
  return count;
}

int fs_close_ldb(int fd) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSE);
#endif
#ifdef LDB_PRINT_CALL
  print_close(fd, true);
#endif

  auto requestId = getNewRequestId();
retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  int rc = fs_close_internal(service, fd, requestId);

  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

  if (rc == 0) {
    auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
    if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
      fprintf(stderr, "LDBLease not found by close %d\n", fd);
      throw std::runtime_error("LDBLease not found by close");
    }
    gLibSharedContext->fdLDBLeaseMap.unsafe_erase(it);
    //    gLibSharedContext->fdLDBLeaseMap.erase(fd);
  }

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSE, tsIdx);
#endif
  return rc;
}

int fs_wsync_internal(FsService *fsServ, int fd, bool isDataSync, off_t offset) {
  struct shmipc_msg msg;
  struct wsyncOp *wsyncOp;
  off_t ring_idx;
  int ret;
  int err;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by fsync %d\n", fd);
    throw std::runtime_error("LDBLease not found by fsync");
  }
  LDBLease *lease = it->second;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  wsyncOp = (struct wsyncOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  msg.type = CFS_OP_WSYNC;
  wsyncOp->fd = fd;
  wsyncOp->off = offset;
  EmbedThreadIdToAsOpRet(wsyncOp->ret);
  void *data_array = lease->GetData(threadMemBuf, &(wsyncOp->array_size),
                                    &(wsyncOp->file_size));
  if (data_array) {
    threadMemBuf->getBufOwnerInfo(data_array, false, wsyncOp->alloc.shmid,
                                  wsyncOp->alloc.dataPtrId, err);
  }

  // shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  if (shmipc_mgr_put_msg_retry_exponential_backoff(fsServ->shmipc_mgr, ring_idx, 
    &msg, gServMngPtr->fsServPid) == -1) {
    print_server_unavailable(__func__);
    threadFsTid = 0;
    ret = fs_retry_pending_ops();
  }

  ret = wsyncOp->ret;
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  if (data_array) {
    fs_free(data_array);
  }
  return ret;
}

int fs_wsync(int fd) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd, true);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  auto offset = service->getOffset(fd);
  ssize_t rc = fs_wsync_internal(service, fd, true, offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fdatasync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}
/* #endregion fs ldb specific */
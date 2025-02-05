#include "FsProc_WorkerComm.h"

#include "FsProc_FsReq.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"

WorkerCommMessage *genFileInodeOutMsg(WorkerCommMessageType tp,
                                      InMemInode *inodePtr, FsReq *req) {
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  auto msgPtr = new WorkerCommMessage(tp);
  msgPtr->targetInode = inodePtr;
  msgPtr->sendMsg = nullptr;
  msgPtr->req = req;
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  return msgPtr;
}

WorkerCommMessage *genFileInodeOutReplyMsg(WorkerCommMessageType tp,
                                           WorkerCommMessage *sendMsg) {
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  auto msgPtr = new WorkerCommMessage(tp);
  assert(checkWkCommMsgReplyTypeMatchSent(sendMsg->msgType, tp));
  msgPtr->targetInode = sendMsg->targetInode;
  msgPtr->sendMsg = sendMsg;
  msgPtr->req = sendMsg->req;
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  return msgPtr;
}

bool WorkerCommBridge::Put(FsProcWorker *putter_worker, WorkerCommMessage *msg,
                           std::shared_ptr<spdlog::logger> curLogger) {
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  auto curSendQueue = getWorkerSendQueue(putter_worker);
  if (curSendQueue == nullptr) {
    const char *warnMsg = "Put() cannot find corresponding sending worker";
    curLogger->warn(warnMsg);
    return false;
  }
  bool rc = curSendQueue->try_enqueue(msg);
  if (rc) {
    waitForReplyMsg_[getWorkerIdx(putter_worker)].emplace(msg);
  }
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  return rc;
}

WorkerCommMessage *WorkerCommBridge::Get(
    FsProcWorker *getter_worker, WorkerCommMessage **msg,
    std::shared_ptr<spdlog::logger> curLogger) {
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  assert(msg != nullptr);
  *msg = nullptr;
  auto curRecvQueue = getWorkerRecvQueue(getter_worker);
  if (curRecvQueue == nullptr) {
    const char *warnMsg = "Get() cannot find corresponding recv worker";
    curLogger->warn(warnMsg);
    return nullptr;
  }

  WorkerCommMessage *msg_ptr = nullptr;
  curRecvQueue->try_dequeue(msg_ptr);
  if (msg_ptr != nullptr) {
    curLogger->debug("get recv staff cur_idx:{}", getWorkerIdx(getter_worker));
    *msg = msg_ptr->sendMsg;
    if (*msg) {
      // delete from waiting pool
      int eraseNum = waitForReplyMsg_[getWorkerIdx(getter_worker)].erase(*msg);
      assert(eraseNum);
    }
  }
  // std::cout << "[BENITA]" << __func__ << "\t" << __LINE__ << std::endl;
  return msg_ptr;
}

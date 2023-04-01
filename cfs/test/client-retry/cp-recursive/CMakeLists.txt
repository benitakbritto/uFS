# ################# test client-retry-cp-recursive #################

add_executable(initClientRetryCpWorkload init.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h)
target_link_libraries(initClientRetryCpWorkload PRIVATE cfs rt ${TBB_LIBRARIES})

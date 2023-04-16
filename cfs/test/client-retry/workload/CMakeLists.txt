# ################# test workload #################

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# seqread 
add_executable(webserver-ro webserver-ro.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(webserver-ro PRIVATE cfs rt ${TBB_LIBRARIES})

# init data files
add_executable(initWorkload init.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(initWorkload PRIVATE cfs rt ${TBB_LIBRARIES})
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(testClient testClient.cc ../../../client/testClient_common.h
                          ../../../../src/util.cc ../../../../include/util.h)
target_link_libraries(testClient PRIVATE cfs rt ${TBB_LIBRARIES})
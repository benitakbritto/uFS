# ################# test workload #################

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# webserver 
add_executable(webserver webserver.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(webserver PRIVATE cfs rt ${TBB_LIBRARIES})

# varmail
add_executable(varmail varmail.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(varmail PRIVATE cfs rt ${TBB_LIBRARIES})

# init data files
add_executable(initWorkload init.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(initWorkload PRIVATE cfs rt ${TBB_LIBRARIES})
# ################# test microbenchmarks #################

# seqread 
add_executable(seqread bench_seqread.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(seqread PRIVATE cfs rt ${TBB_LIBRARIES})

# seqwrite
add_executable(seqwrite bench_seqwrite.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(seqwrite PRIVATE cfs rt ${TBB_LIBRARIES})

# random read
add_executable(rread bench_rread.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(rread PRIVATE cfs rt ${TBB_LIBRARIES})

# random write
add_executable(rwrite bench_rwrite.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(rwrite PRIVATE cfs rt ${TBB_LIBRARIES})

# random write
add_executable(rwritesync bench_rwritesync.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(rwritesync PRIVATE cfs rt ${TBB_LIBRARIES})

# seq write random size
add_executable(seqwriterand bench_seqwriterand.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(seqwriterand PRIVATE cfs rt ${TBB_LIBRARIES})

# create
add_executable(create bench_create.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(create PRIVATE cfs rt ${TBB_LIBRARIES})

# create files
add_executable(createfiles bench_createfiles.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(createfiles PRIVATE cfs rt ${TBB_LIBRARIES})

# stat
add_executable(stat bench_stat.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(stat PRIVATE cfs rt ${TBB_LIBRARIES})

# init data files
add_executable(init init.cc ../../client/testClient_common.h
                          ../../../src/util.cc ../../../include/util.h
                          common.cc common.h)
target_link_libraries(init PRIVATE cfs rt ${TBB_LIBRARIES})
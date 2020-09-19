add_executable(search search.cpp)
target_link_libraries(search milvus_sdk pthread)
install(TARGETS search DESTINATION test)




add_executable(insert insert.cpp)
target_link_libraries(insert milvus_sdk pthread)
install(TARGETS insert DESTINATION test)




add_executable(delete delete.cpp)
target_link_libraries(delete milvus_sdk pthread)
install(TARGETS delete DESTINATION test)

add_executable(create CreateCollection.cpp)
target_link_libraries(create milvus_sdk pthread)
install(TARGETS create DESTINATION test)
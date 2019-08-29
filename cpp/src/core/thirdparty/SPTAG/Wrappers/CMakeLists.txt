# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

find_package(Python2 COMPONENTS Development)
if (Python2_FOUND)    
    include_directories (${Python2_INCLUDE_DIRS})
    link_directories (${Python2_LIBRARY_DIRS})
	set (Python_INCLUDE_DIRS ${Python2_INCLUDE_DIRS})
    set (Python_LIBRARIES ${Python2_LIBRARIES})
	set (Python_FOUND true)
else()
    find_package(Python3 COMPONENTS Development)
    if (Python3_FOUND)
        include_directories (${Python3_INCLUDE_DIRS})
        link_directories (${Python3_LIBRARY_DIRS})
		set (Python_INCLUDE_DIRS ${Python3_INCLUDE_DIRS})
        set (Python_LIBRARIES ${Python3_LIBRARIES})
		set (Python_FOUND true)
    endif()
endif()

if (Python_FOUND)
	message (STATUS "Found Python.")
    message (STATUS "Include Path: ${Python_INCLUDE_DIRS}")
    message (STATUS "Library Path: ${Python_LIBRARIES}")
	
	if (WIN32)
		set(PY_SUFFIX .pyd)
	else()
		set(PY_SUFFIX .so)
	endif()

	execute_process(COMMAND swig -python -c++ -I${PROJECT_SOURCE_DIR}/Wrappers/inc -o ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface_pwrap.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/PythonCore.i)
	execute_process(COMMAND swig -python -c++ -I${PROJECT_SOURCE_DIR}/Wrappers/inc -o ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface_pwrap.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/PythonClient.i)

	include_directories(${PYTHON_INCLUDE_PATH} ${PROJECT_SOURCE_DIR}/AnnService ${PROJECT_SOURCE_DIR}/Wrappers)

	file(GLOB CORE_HDR_FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface.h)
	file(GLOB CORE_SRC_FILES ${PROJECT_SOURCE_DIR}/Wrappers/src/CoreInterface.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface_pwrap.cpp)
	add_library (_SPTAG SHARED ${CORE_SRC_FILES} ${CORE_HDR_FILES})
	set_target_properties(_SPTAG PROPERTIES PREFIX "" SUFFIX ${PY_SUFFIX})
	target_link_libraries(_SPTAG SPTAGLib ${Python_LIBRARIES} ${TBB_LIBRARIES})
	add_custom_command(TARGET _SPTAG POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/Wrappers/inc/SPTAG.py ${EXECUTABLE_OUTPUT_PATH})

	file(GLOB CLIENT_HDR_FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface.h ${PROJECT_SOURCE_DIR}/AnnService/inc/Socket/*.h ${PROJECT_SOURCE_DIR}/AnnService/inc/Client/*.h)
	file(GLOB CLIENT_SRC_FILES ${PROJECT_SOURCE_DIR}/Wrappers/src/ClientInterface.cpp ${PROJECT_SOURCE_DIR}/AnnService/src/Socket/*.cpp ${PROJECT_SOURCE_DIR}/AnnService/src/Client/*.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface_pwrap.cpp)
	add_library (_SPTAGClient SHARED ${CLIENT_SRC_FILES} ${CLIENT_HDR_FILES})
	set_target_properties(_SPTAGClient PROPERTIES PREFIX "" SUFFIX ${PY_SUFFIX})
	target_link_libraries(_SPTAGClient SPTAGLib ${Python_LIBRARIES} ${Boost_LIBRARIES} ${TBB_LIBRARIES})
	add_custom_command(TARGET _SPTAGClient POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/Wrappers/inc/SPTAGClient.py ${EXECUTABLE_OUTPUT_PATH})

	install(TARGETS _SPTAG _SPTAGClient
	RUNTIME DESTINATION bin  
	ARCHIVE DESTINATION lib
	LIBRARY DESTINATION lib)
	install(FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/SPTAG.py ${PROJECT_SOURCE_DIR}/Wrappers/inc/SPTAGClient.py DESTINATION bin)
else()
	message (STATUS "Could not find Python.")
endif()

find_package(JNI)
if (!JNI_FOUND)
    include_directories (${JNI_INCLUDE_DIRS})
    link_directories (${JNI_LIBRARY_DIRS})
    message (STATUS "Found JNI.")
    message (STATUS "Include Path: ${JNI_INCLUDE_DIRS}")
    message (STATUS "Library Path: ${JNI_LIBRARIES}")

	if (WIN32)
		set (JAVA_SUFFIX .dll)
	else()
		set (JAVA_SUFFIX .so)
	endif()
	
	execute_process(COMMAND swig -java -c++ -I${PROJECT_SOURCE_DIR}/Wrappers/inc -o ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface_jwrap.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/JavaCore.i)
	execute_process(COMMAND swig -java -c++ -I${PROJECT_SOURCE_DIR}/Wrappers/inc -o ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface_jwrap.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/JavaClient.i)
	
	include_directories(${JNI_INCLUDE_DIRS} ${PROJECT_SOURCE_DIR}/AnnService ${PROJECT_SOURCE_DIR}/Wrappers)

	file(GLOB CORE_HDR_FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface.h)
	file(GLOB CORE_SRC_FILES ${PROJECT_SOURCE_DIR}/Wrappers/src/CoreInterface.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/CoreInterface_jwrap.cpp)
	add_library (SPTAG SHARED ${CORE_SRC_FILES} ${CORE_HDR_FILES})
	set_target_properties(SPTAG PROPERTIES SUFFIX ${JAVA_SUFFIX})
	target_link_libraries(SPTAG SPTAGLib ${JNI_LIBRARIES} ${TBB_LIBRARIES})

	file(GLOB CLIENT_HDR_FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface.h ${PROJECT_SOURCE_DIR}/AnnService/inc/Socket/*.h ${PROJECT_SOURCE_DIR}/AnnService/inc/Client/*.h)
	file(GLOB CLIENT_SRC_FILES ${PROJECT_SOURCE_DIR}/Wrappers/src/ClientInterface.cpp ${PROJECT_SOURCE_DIR}/AnnService/src/Socket/*.cpp ${PROJECT_SOURCE_DIR}/AnnService/src/Client/*.cpp ${PROJECT_SOURCE_DIR}/Wrappers/inc/ClientInterface_jwrap.cpp)
	add_library (SPTAGClient SHARED ${CLIENT_SRC_FILES} ${CLIENT_HDR_FILES})
	set_target_properties(SPTAGClient PROPERTIES SUFFIX ${JAVA_SUFFIX})
	target_link_libraries(SPTAGClient SPTAGLib ${JNI_LIBRARIES} ${Boost_LIBRARIES} ${TBB_LIBRARIES})
	
	file(GLOB JAVA_FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/*.java)
	foreach(JAVA_FILE ${JAVA_FILES})
	    message (STATUS "Add copy post-command for file " ${JAVA_FILE})
	    add_custom_command(TARGET SPTAGClient POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${JAVA_FILE} ${EXECUTABLE_OUTPUT_PATH})
	endforeach(JAVA_FILE)
	
	install(TARGETS SPTAG SPTAGClient
	RUNTIME DESTINATION bin  
	ARCHIVE DESTINATION lib
	LIBRARY DESTINATION lib)
	install(FILES ${PROJECT_SOURCE_DIR}/Wrappers/inc/*.java DESTINATION bin)
else()
	message (STATUS "Could not find JNI.")
endif()
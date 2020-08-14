if( ${UNIX} )
	option(NGT_SHARED_MEMORY_ALLOCATOR "enable shared memory" OFF)
	configure_file(${CMAKE_CURRENT_SOURCE_DIR}/defines.h.in ${CMAKE_CURRENT_BINARY_DIR}/defines.h)
	include_directories("${CMAKE_CURRENT_BINARY_DIR}" "${PROJECT_SOURCE_DIR}/lib" "${PROJECT_BINARY_DIR}/lib/")

	file(GLOB NGT_SOURCES *.cpp)
	file(GLOB HEADER_FILES *.h *.hpp)
	file(GLOB NGTQ_HEADER_FILES NGTQ/*.h NGTQ/*.hpp)

	add_library(ngtstatic STATIC ${NGT_SOURCES})
	set_target_properties(ngtstatic PROPERTIES OUTPUT_NAME ngt) 
	set_target_properties(ngtstatic PROPERTIES COMPILE_FLAGS "-fPIC")
	target_link_libraries(ngtstatic)
	if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
		target_link_libraries(ngtstatic OpenMP::OpenMP_CXX)
	endif()

	add_library(ngt SHARED ${NGT_SOURCES})
	set_target_properties(ngt PROPERTIES VERSION ${ngt_VERSION}) 
	set_target_properties(ngt PROPERTIES SOVERSION ${ngt_SOVERSION}) 
	add_dependencies(ngt ngtstatic) 
	if(${APPLE})
		if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
			target_link_libraries(ngt OpenMP::OpenMP_CXX)
		else()
			target_link_libraries(ngt gomp)
		endif()
	else(${APPLE})
		target_link_libraries(ngt gomp rt)
	endif(${APPLE})

	add_custom_command(OUTPUT command DEPENDS ${NGT_SOURCES} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND sh ${PROJECT_SOURCE_DIR}/utils/mk_version_defs_h.sh ${PROJECT_SOURCE_DIR} version_defs.h)
	add_custom_target(versiondef DEPENDS command)
	add_dependencies(ngtstatic versiondef)

	install(TARGETS
		ngt
		ngtstatic
		RUNTIME DESTINATION bin
		LIBRARY DESTINATION lib
		ARCHIVE DESTINATION lib)
	install(FILES ${HEADER_FILES} ${CMAKE_CURRENT_BINARY_DIR}/defines.h  ${CMAKE_CURRENT_BINARY_DIR}/version_defs.h DESTINATION include/NGT)
	install(FILES ${NGTQ_HEADER_FILES} DESTINATION include/NGT/NGTQ)
endif()

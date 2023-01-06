cmake_minimum_required(VERSION 3.17.0)

project(libhdfs3)

if (false)
if (NOT USE_INTERNAL_PROTOBUF_LIBRARY)
    # compatiable with protobuf which was compiled old C++ ABI
    set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
    set(CMAKE_C_FLAGS "")
    if (NOT (CMAKE_VERSION VERSION_LESS "3.8.0"))
        unset(CMAKE_CXX_STANDARD)
    endif ()
endif()
endif()

SET(WITH_KERBEROS false)
SET(WITH_CURL false)
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wl,-rpath,'$$ORIGIN' -Wl,-z,origin -ggdb3 -O0")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,'$$ORIGIN' -Wl,-z,origin -std=c++2a -ggdb3 -O0")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wl,-rpath,'$$ORIGIN' -Wl,-z,origin -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,'$$ORIGIN' -Wl,-z,origin -std=c++2a -O3")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# project and source dir
set(HDFS3_ROOT_DIR ${CMAKE_SOURCE_DIR}/)
set(HDFS3_SOURCE_DIR ${HDFS3_ROOT_DIR}/src)
set(HDFS3_COMMON_DIR ${HDFS3_SOURCE_DIR}/common)


# module
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
include(Platform)
include(Options)
include(find_protobuf)
include(find_libgsasl)
include(find_libxml2)
include(find_rapidjson)
include(find_boost)
include(find_metrics2)
include(find_msgpack)

add_subdirectory(contrib/infsec)
add_subdirectory(contrib/libxml2-cmake)
add_subdirectory(contrib/libgsasl)
add_subdirectory(contrib/boost-cmake)

# prefer shared libraries
if (WITH_KERBEROS)
    find_package(KERBEROS REQUIRED)
endif()

if (WITH_CURL)
    find_package(CURL REQUIRED)
endif()

# source
set(PROTO_FILES
    ${HDFS3_SOURCE_DIR}/proto/encryption.proto
    ${HDFS3_SOURCE_DIR}/proto/ClientDatanodeProtocol.proto
    ${HDFS3_SOURCE_DIR}/proto/hdfs.proto
    ${HDFS3_SOURCE_DIR}/proto/Security.proto
    ${HDFS3_SOURCE_DIR}/proto/ProtobufRpcEngine.proto
    ${HDFS3_SOURCE_DIR}/proto/ClientNamenodeProtocol.proto
    ${HDFS3_SOURCE_DIR}/proto/IpcConnectionContext.proto
    ${HDFS3_SOURCE_DIR}/proto/RpcHeader.proto
    ${HDFS3_SOURCE_DIR}/proto/datatransfer.proto
    )

PROTOBUF_GENERATE_CPP(PROTO_SOURCES PROTO_HEADERS ${PROTO_FILES})

configure_file(${HDFS3_SOURCE_DIR}/platform.h.in ${CMAKE_CURRENT_BINARY_DIR}/platform.h)
configure_file(${HDFS3_SOURCE_DIR}/platform.h.in ${HDFS3_SOURCE_DIR}/platform.h)


file(GLOB_RECURSE SRCS
	        "${HDFS3_SOURCE_DIR}/*.h"
		        "${HDFS3_SOURCE_DIR}/*.cpp"
			        )

# target
add_library(hdfs3 STATIC ${SRCS} ${PROTO_SOURCES} ${PROTO_HEADERS})

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_dependencies(hdfs3 protoc)
endif()

target_include_directories(hdfs3 PRIVATE ${HDFS3_SOURCE_DIR})
target_include_directories(hdfs3 PRIVATE ${HDFS3_COMMON_DIR})
target_include_directories(hdfs3 PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(hdfs3 PRIVATE ${HDFS3_ROOT_DIR})
target_include_directories(hdfs3 PRIVATE ${HDFS3_ROOT_DIR}/contrib)
target_include_directories(hdfs3 PRIVATE ${METRICS_INCLUDE_DIR})
target_include_directories(hdfs3 PRIVATE ${MSGPACK_INCLUDE_DIR})

target_include_directories(hdfs3 PRIVATE ${LIBGSASL_INCLUDE_DIR})
if (WITH_KERBEROS)
    target_include_directories(hdfs3 PRIVATE ${KERBEROS_INCLUDE_DIRS})
endif()

if (WITH_CURL)
    target_include_directories(hdfs3 PRIVATE ${CURL_INCLUDE_DIRS})
endif()

target_include_directories(hdfs3 PRIVATE ${LIBXML2_INCLUDE_DIR})

target_include_directories(hdfs3 PRIVATE ${INFSEC_INCLUDE_DIR})

target_link_libraries(hdfs3 ${LIBGSASL_LIBRARY})
if (WITH_KERBEROS)
    target_link_libraries(hdfs3 ${KERBEROS_LIBRARIES})
endif()

if (WITH_CURL)
    target_link_libraries(hdfs3 ${CURL_LIBRARIES})
endif()

target_link_libraries(hdfs3 ${LIBXML2_LIBRARY})

target_link_libraries(hdfs3 ${INFSEC_LIBRARIES})

# inherit from parent cmake
target_include_directories(hdfs3 PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(hdfs3 PRIVATE ${Protobuf_INCLUDE_DIR})

#poco logger
target_include_directories(hdfs3 PRIVATE ${Poco_INCLUDE_DIRS})
target_link_libraries(hdfs3 ${Poco_Foundation_LIBRARY})

# boost
target_link_libraries(hdfs3 ${Boost_SYSTEM_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_IOSTREAMS_LIBRARY} ${Boost_FILESYSTEM_LIBRARY} ${Boost_REGEX_LIBRARY} ${})

# libgsasl
#target_compile_definitions (hdfs3 PRIVATE HAVE_CONFIG_H=1)

target_link_libraries(hdfs3 ${Protobuf_LIBRARY})
if(OPENSSL_INCLUDE_DIR AND OPENSSL_LIBRARIES)
    target_include_directories(hdfs3 PRIVATE ${OPENSSL_INCLUDE_DIR})
    target_link_libraries(hdfs3 ${OPENSSL_LIBRARIES})
endif()

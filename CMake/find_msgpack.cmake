option(ENABLE_MSGPACK "Use msgpack" ON)
if(NOT ENABLE_MSGPACK)
    return()
endif()

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/contrib/msgpack/include/msgpack.hpp")
    set(MISSING_INTERNAL_MSGPACK_LIBRARY 1)
endif()

if(NOT MISSING_INTERNAL_MSGPACK_LIBRARY)
    set(MSGPACK_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/contrib/msgpack/include")
    set(USE_MSGPACK 1)
endif()

message(STATUS "Using msgpack=${USE_MSGPACK}: ${MSGPACK_INCLUDE_DIR}")

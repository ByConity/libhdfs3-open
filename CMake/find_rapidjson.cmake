option(ENABLE_RAPIDJSON "Use rapidjson" ON)
if(NOT ENABLE_RAPIDJSON)
    return()
endif()

option(USE_INTERNAL_RAPIDJSON_LIBRARY "Set to FALSE to use system rapidjson library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/contrib/rapidjson/rapidjson.h")
    if(USE_INTERNAL_RAPIDJSON_LIBRARY)
       message(WARNING "submodule contrib/rapidjson is missing. to fix try run: \n git submodule update --init --recursive")
       set(USE_INTERNAL_RAPIDJSON_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_RAPIDJSON_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_RAPIDJSON_LIBRARY)
    find_path(RAPIDJSON_INCLUDE_DIR NAMES rapidjson/rapidjson.h PATHS ${RAPIDJSON_INCLUDE_PATHS})
endif()

if(RAPIDJSON_INCLUDE_DIR)
    set(USE_RAPIDJSON 1)
elseif(NOT MISSING_INTERNAL_RAPIDJSON_LIBRARY)
    set(RAPIDJSON_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/contrib/rapidjson")
    set(USE_INTERNAL_RAPIDJSON_LIBRARY 1)
    set(USE_RAPIDJSON 1)
endif()

message(STATUS "Using rapidjson=${USE_RAPIDJSON}: ${RAPIDJSON_INCLUDE_DIR}")

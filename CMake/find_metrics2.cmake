option(ENABLE_METRICS "Use metrics2" ON)
if(NOT ENABLE_METRICS)
    return()
endif()

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/contrib/metrics2/metrics.h")
    set(MISSING_INTERNAL_METRICS_LIBRARY 1)
endif()

if(NOT MISSING_INTERNAL_METRICS_LIBRARY)
    set(METRICS_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/contrib/metrics2")
    set(USE_METRICS 1)
endif()

message(STATUS "Using metrics2=${USE_METRICS}: ${METRICS_INCLUDE_DIR}")

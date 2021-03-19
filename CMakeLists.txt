cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(wan_agent CXX)

# Version
set(wan_agent_VERSION 0.1)
set(wan_agent_build_VERSION 0.1.1)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Wall -ggdb -gdwarf-3")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ()

# mutils_FOUND 
# mutils_INCLUDE_DIRS
# mutils_LIBRARIES
find_package(mutils REQUIRED)

# spdlog_FOUND
find_package(spdlog REQUIRED)

# openssl_FOUND
find_package(OpenSSL REQUIRED)

# derecho
find_package(derecho REQUIRED)

# json
find_package(nlohmann_json 3.2.0 REQUIRED)

# predicate_generator
find_package(predicate_generator REQUIRED)

include(CheckIncludeFiles)

add_library(wan_logger OBJECT src/impl/logger_impl.cpp)
target_include_directories(wan_logger PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent_utils OBJECT src/impl/wan_agent_utils.cpp)
target_include_directories(wan_agent_utils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent_object OBJECT src/impl/wan_agent_object.cpp)
target_include_directories(wan_agent_object PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent SHARED 
    src/impl/wan_agent_impl.cpp
    $<TARGET_OBJECTS:wan_logger>
    $<TARGET_OBJECTS:wan_agent_utils>
    $<TARGET_OBJECTS:wan_agent_object>
)
set_target_properties(wan_agent PROPERTIES PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/include/wan_agent.hpp)


target_include_directories(wan_agent PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_link_libraries(wan_agent
    ${derecho_LIBRARIES}
    ${mutils_LIBRARIES} 
    ${OPENSSL_LIBRARIES}
    predicate_generator
    )

install(TARGETS wan_agent
    EXPORT wan_agentTargets
    LIBRARY DESTINATION lib
)
# 把所有的头文件都给拷贝过去
install(DIRECTORY
        include/
        DESTINATION include/wan_agent)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    wan_agentConfigVersion.cmake
    VERSION ${wan_agent_VERSION}
    COMPATIBILITY AnyNewerVersion
)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
install(EXPORT wan_agentTargets
        FILE wan_agentTargets.cmake
        NAMESPACE wan_agent::
        DESTINATION lib/cmake/wan_agent
        )

set(ConfigPackageLocation lib/cmake/wan_agent)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

configure_file (wan_agentConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfig.cmake"
)
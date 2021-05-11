cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_executable(wan_agent_server wan_agent_server.cpp
)
target_include_directories(wan_agent_server PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_server wan_agent pthread mutils derecho crypto)

#add_executable(wan_agent_client wan_agent_client.cpp
#)
#target_include_directories(wan_agent_client PUBLIC
#    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
#    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
#    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
#)
#target_link_libraries(wan_agent_client wan_agent)

add_executable(wan_agent_complete wan_agent.cpp
)
target_include_directories(wan_agent_complete PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_complete wan_agent)

add_executable(read_test read_test.cpp
)
target_include_directories(read_test PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(read_test wan_agent)

add_executable(change_with_time change_with_time.cpp)
target_include_directories(change_with_time PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(change_with_time wan_agent)
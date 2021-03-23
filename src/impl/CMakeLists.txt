cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(wan_logger OBJECT logger_impl.cpp)
target_include_directories(wan_logger PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent_utils OBJECT wan_agent_utils.cpp)
target_include_directories(wan_agent_utils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent SHARED 
    wan_agent_impl.cpp
    $<TARGET_OBJECTS:wan_logger>
    $<TARGET_OBJECTS:wan_agent_utils>
)

target_include_directories(wan_agent PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
# target_link_libraries(wan_agent wan_logger wan_agent_utils)
# target_link_libraries(wan_agent predicate_generator)
target_link_libraries(wan_agent
    ${derecho_LIBRARIES}
    ${mutils_LIBRARIES} 
    ${OPENSSL_LIBRARIES}
    predicate_generator
    )

# add_custom_command(TARGET cascade_wan_sender POST_BUILD
#     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
#     ${CMAKE_CURRENT_BINARY_DIR}/cfg
# )
# add_custom_command(TARGET cascade_wan_sender POST_BUILD
#     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/larger_cfg
#     ${CMAKE_CURRENT_BINARY_DIR}/larger_cfg
# )
# add_custom_command(TARGET cascade_wan_sender POST_BUILD
#     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/big_shard_cfg
#     ${CMAKE_CURRENT_BINARY_DIR}/big_shard_cfg
# )

# make libcascade.so
# target_link_libraries(wan_agent
#     ${derecho_LIBRARIES}
#     ${mutils_LIBRARIES} 
#     ${OPENSSL_LIBRARIES}
#     predicate_generator
#     )

set_target_properties(wan_agent PROPERTIES
    SOVERSION ${wan_agent_VERSION}
    VERSION ${wan_agent_build_VERSION}
)

# add_dependencies(cascade core)
#    utils core)

# make install

# install(TARGETS wan_agent 
#         EXPORT wan_agentTargets
#         LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#         PUBLIC_HEADER DESTINATION include
#     )

# install(DIRECTORY
#         include
#         DESTINATION include)

# install(FILES
#         ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h
#         DESTINATION include/cascade)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfigVersion.cmake"
    VERSION ${wan_agent_VERSION}
    COMPATIBILITY AnyNewerVersion
)

# export (EXPORT wan_agent
#     FILE "${CMAKE_CURRENT_BINARY_DIR}/wan_agentTargets.cmake")
set(ConfigPackageLocation lib/cmake/wan_agent)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

configure_file (wan_agentConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/wan_agentConfig.cmake"
)




# install(EXPORT wan_logger
#     FILE wan_loggerTargets.cmake
#     DESTINATION ${ConfigPackageLocation}
# )
# install(EXPORT wan_agent_utils
#     FILE wan_agent_utilsTargets.cmake
#     DESTINATION ${ConfigPackageLocation}
# )

install(EXPORT wan_agent
    FILE wan_agentTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)



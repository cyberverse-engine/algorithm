cmake_minimum_required(VERSION 4.0.0)

add_library(cyberverse_algorithm_static_lib STATIC)
add_library(cyberverse::algorithm::static_lib ALIAS cyberverse_algorithm_static_lib)

target_include_directories(cyberverse_algorithm_static_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

# Header files
set(
  HEADER_FILES
  cyberverse/algorithm.hpp
  cyberverse/algorithm/container/fixed_ring_buffer.hpp
  cyberverse/algorithm/container/sharded_queue.hpp
  cyberverse/algorithm/container/spsc_queue.hpp
  cyberverse/algorithm/container/triple_buffer.hpp
  cyberverse/algorithm/detail/bounded_mpmc_queue.hpp
)

# Source files
set(
  SOURCE_FILES
  cyberverse/algorithm/algorithm.cpp
)

target_sources(cyberverse_algorithm_static_lib PUBLIC ${HEADER_FILES})
target_sources(cyberverse_algorithm_static_lib PRIVATE ${SOURCE_FILES})

target_compile_features(cyberverse_algorithm_static_lib PUBLIC cxx_std_23)

find_package(Threads REQUIRED)

target_link_libraries(
  cyberverse_algorithm_static_lib
  PUBLIC
  Threads::Threads
)

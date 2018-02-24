cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(Sphinx LANGUAGES CXX)

enable_testing()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

find_package(RAGEL 6.6 REQUIRED)

set(SPHINX_VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")

set(CMAKE_CXX_FLAGS "-O3 -g -Wall -Wextra")

execute_process(
  COMMAND git log -1 --format=%h
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_COMMIT_HASH
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

configure_file(
  ${CMAKE_SOURCE_DIR}/version.h.in
  ${CMAKE_BINARY_DIR}/generated/version.h
)

include_directories(${CMAKE_BINARY_DIR}/generated)

add_subdirectory(sphinxd)

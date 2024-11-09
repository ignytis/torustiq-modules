cmake_minimum_required(VERSION 3.10)

project(torustiq-kafka-cpp)

set(CMAKE_CXX_STANDARD 17)

# Add the source files to the project
set(SOURCE_FILES
    src/main.cpp
    src/producer.cpp
)

# Add the header files to the project
set(HEADER_FILES
    ../torustiq_common_typedefs.hpp
    src/producer.hpp
)

# Create the shared library (DLL)
add_library(torustiq-kafka-cpp SHARED ${HEADER_FILES} ${SOURCE_FILES})

target_link_libraries(torustiq-kafka-cpp rdkafka++)

set_target_properties(torustiq-kafka-cpp
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target
)
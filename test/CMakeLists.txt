if(DEFINED ENV{PG_HOST})
    set(POSTGRES_HOST $ENV{PG_HOST})
else()
    set(POSTGRES_HOST localhost)
endif()

message("POSTGRES_HOST=${POSTGRES_HOST}")

add_definitions (
        -DTEST_DB_URL="postgresql://postgres:db-pass@${POSTGRES_HOST}:5432/postgres"
        -DTEST_DB_MIGRATION="${CMAKE_CURRENT_SOURCE_DIR}/oatpp-postgresql/migration/"
)

add_executable(module-tests
        oatpp-postgresql/ql_template/ParserTest.cpp
        oatpp-postgresql/ql_template/ParserTest.hpp
        oatpp-postgresql/types/ArrayTest.cpp
        oatpp-postgresql/types/ArrayTest.hpp
        oatpp-postgresql/types/FloatTest.cpp
        oatpp-postgresql/types/FloatTest.hpp
        oatpp-postgresql/types/InterpretationTest.cpp
        oatpp-postgresql/types/InterpretationTest.hpp
        oatpp-postgresql/types/IntTest.cpp
        oatpp-postgresql/types/IntTest.hpp
        oatpp-postgresql/tests.cpp
        )

set_target_properties(module-tests PROPERTIES
        CXX_STANDARD 11
        CXX_EXTENSIONS OFF
        CXX_STANDARD_REQUIRED ON
)

target_include_directories(module-tests
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

if(OATPP_MODULES_LOCATION STREQUAL OATPP_MODULES_LOCATION_EXTERNAL)
    add_dependencies(module-tests ${LIB_OATPP_EXTERNAL})
endif()

add_dependencies(module-tests ${OATPP_THIS_MODULE_NAME})

target_link_oatpp(module-tests)

target_link_libraries(module-tests
        PRIVATE ${OATPP_THIS_MODULE_NAME}
)

## TODO link dependencies here (if some)

add_test(module-tests module-tests)
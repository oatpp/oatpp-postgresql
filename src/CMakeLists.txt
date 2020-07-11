
add_library(${OATPP_THIS_MODULE_NAME}
        oatpp-postgresql/Connection.cpp
        oatpp-postgresql/Connection.hpp
        oatpp-postgresql/Executor.cpp
        oatpp-postgresql/Executor.hpp
        oatpp-postgresql/ql_template/Parser.cpp
        oatpp-postgresql/ql_template/Parser.hpp
        oatpp-postgresql/ql_template/TemplateValueProvider.cpp
        oatpp-postgresql/ql_template/TemplateValueProvider.hpp
        oatpp-postgresql/mapping/Deserializer.cpp
        oatpp-postgresql/mapping/Deserializer.hpp
        oatpp-postgresql/mapping/Serializer.cpp
        oatpp-postgresql/mapping/Serializer.hpp
)

set_target_properties(${OATPP_THIS_MODULE_NAME} PROPERTIES
        CXX_STANDARD 11
        CXX_EXTENSIONS OFF
        CXX_STANDARD_REQUIRED ON
)

if(OATPP_MODULES_LOCATION STREQUAL OATPP_MODULES_LOCATION_EXTERNAL)
    add_dependencies(${OATPP_THIS_MODULE_NAME} ${LIB_OATPP_EXTERNAL})
endif()

target_link_oatpp(${OATPP_THIS_MODULE_NAME})

target_include_directories(${OATPP_THIS_MODULE_NAME}
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_link_libraries(${OATPP_THIS_MODULE_NAME}
        PUBLIC PostgreSQL::PostgreSQL
)

#######################################################################################################
## install targets

if(OATPP_INSTALL)
    include("../cmake/module-install.cmake")
endif()

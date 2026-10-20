cmake_minimum_required(VERSION 3.21)

# The package version is human-owned, like cpp/moq/VERSION; release-c.yml tags c-v<it>.
file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/VERSION" _version LIMIT_COUNT 1)
if(NOT _version MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
    message(FATAL_ERROR "Unsupported version '${_version}' in c/moq/VERSION (want x.y.z)")
endif()

# The API is C, but its implementation is the generated C++ bindings, compiled here.
project(moq-c VERSION ${_version} LANGUAGES C CXX)

set(_workspace "${CMAKE_CURRENT_SOURCE_DIR}/../..")
set(MOQ_FFI_DIR "" CACHE PATH "Extracted default-feature moq-ffi native package")
if(MOQ_FFI_DIR)
    get_filename_component(MOQ_FFI_DIR "${MOQ_FFI_DIR}" ABSOLUTE)
    file(READ "${MOQ_FFI_DIR}/manifest.json" _manifest)
    foreach(_field version target features profile source)
        string(JSON _input_${_field} GET "${_manifest}" ${_field})
    endforeach()
    if(NOT _input_features STREQUAL "default" OR NOT _input_profile STREQUAL "release")
        message(FATAL_ERROR "Prebuilt moq-ffi requires default features and release profile")
    endif()
    if(NOT _input_version MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z.+-]+)?$")
        message(FATAL_ERROR "Malformed prebuilt moq-ffi version: ${_input_version}")
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
            set(_ffi_target aarch64-unknown-linux-gnu)
        elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
            set(_ffi_target x86_64-unknown-linux-gnu)
        endif()
    elseif(APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        set(_ffi_target aarch64-apple-darwin)
    elseif(MSVC AND CMAKE_SIZEOF_VOID_P EQUAL 8 AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(AMD64|x86_64)$")
        set(_ffi_target x86_64-pc-windows-msvc)
    endif()
    if(NOT DEFINED _ffi_target OR NOT _ffi_target STREQUAL _input_target)
        message(FATAL_ERROR "Prebuilt moq-ffi target ${_input_target} does not match this host")
    endif()
    set(_cargo_target "${MOQ_FFI_DIR}/lib")
    set(_profile "")
else()
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(_profile debug)
        set(_cargo_flags "")
    else()
        set(_profile release)
        set(_cargo_flags --release)
    endif()
    # A build target, even the host's, moves the library under <triple>/, which cargo metadata
    # does not report. A `[build] target` in a cargo config fails later, at the missing library.
    if(DEFINED ENV{CARGO_BUILD_TARGET})
        message(FATAL_ERROR "Unsupported CARGO_BUILD_TARGET: moq-ffi builds for the host here; unset it")
    endif()
    # Ask cargo, so CARGO_TARGET_DIR (even relative) and a configured build.target-dir both land
    # where the build below puts the library.
    execute_process(
        COMMAND cargo metadata --locked --no-deps --format-version 1
        WORKING_DIRECTORY "${_workspace}"
        OUTPUT_VARIABLE _cargo_metadata
        COMMAND_ERROR_IS_FATAL ANY
    )
    string(JSON _cargo_target GET "${_cargo_metadata}" target_directory)
endif()

if(WIN32)
    set(MOQ_FFI_LIB "${_cargo_target}/${_profile}/moq_ffi.lib")
    set(_metadata "${_cargo_target}/${_profile}/moq_ffi.dll")
elseif(APPLE)
    set(MOQ_FFI_LIB "${_cargo_target}/${_profile}/libmoq_ffi.a")
    set(_metadata "${_cargo_target}/${_profile}/libmoq_ffi.dylib")
else()
    set(MOQ_FFI_LIB "${_cargo_target}/${_profile}/libmoq_ffi.a")
    set(_metadata "${_cargo_target}/${_profile}/libmoq_ffi.so")
endif()
if(MOQ_FFI_DIR)
    foreach(_input "${MOQ_FFI_LIB}" "${_metadata}" "${MOQ_FFI_DIR}/native-libs.txt")
        if(NOT EXISTS "${_input}")
            message(FATAL_ERROR "Missing prebuilt FFI input: ${_input}")
        endif()
    endforeach()
    set(_cargo_ffi "")
else()
    # The package ships the OpenH264 software fallback, like the C++ package and the prebuilt
    # input do.
    # One cargo target per build tree, so a project adding both c/moq and cpp/moq (which build
    # the same moq-ffi) has one rule for its outputs, as Ninja requires.
    if(NOT TARGET moq_ffi_cargo)
        add_custom_target(moq_ffi_cargo
            COMMAND cargo build --locked --package moq-ffi --features openh264 ${_cargo_flags}
            WORKING_DIRECTORY "${_workspace}"
            BYPRODUCTS "${MOQ_FFI_LIB}" "${_metadata}"
            COMMENT "Building moq-ffi"
            VERBATIM
        )
    endif()
    set(_cargo_ffi moq_ffi_cargo)
endif()

find_program(UNIFFI_BINDGEN_CPP uniffi-bindgen-cpp REQUIRED)

# The C backend renders the C header, its C++ implementation, and the C++ bindings that
# implementation calls. It needs no config: C names are moq-ffi's own. Metadata comes from the cdylib, like the C++ package, while linking
# uses the staticlib. Rendered to a scratch directory and copied only when changed, so a no-op
# cargo build does not recompile the bindings.
set(_generated "${CMAKE_CURRENT_BINARY_DIR}/include/moq/ffi")
set(_generated_files moq.h moq_c.cpp moq.hpp moq.cpp moq_scaffolding.hpp uniffi_expected.hpp)
list(TRANSFORM _generated_files PREPEND "${_generated}/" OUTPUT_VARIABLE _generated_paths)
set(_render "${CMAKE_CURRENT_BINARY_DIR}/render")
list(TRANSFORM _generated_files PREPEND "${_render}/" OUTPUT_VARIABLE _render_paths)
# Named apart from cpp/moq's `moq_ffi`, so both packages can build in one tree.
add_custom_target(moq_c_ffi ALL
    COMMAND ${CMAKE_COMMAND} -E rm -rf "${_render}"
    COMMAND "${UNIFFI_BINDGEN_CPP}" --lang c --library "${_metadata}" --out-dir "${_render}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${_generated}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${_render_paths} "${_generated}"
    WORKING_DIRECTORY "${_workspace}"
    BYPRODUCTS ${_generated_paths}
    COMMENT "Rendering the moq-ffi C bindings"
    VERBATIM
)
if(_cargo_ffi)
    add_dependencies(moq_c_ffi ${_cargo_ffi})
endif()

# System libraries the staticlib needs from an external linker. moq-ffi and moq-c share one
# dependency tree, so they share rs/moq-c/native-libs/, which says how to regenerate it.
if(APPLE)
    set(_platform apple)
elseif(WIN32)
    set(_platform windows)
else()
    set(_platform linux)
endif()
if(MOQ_FFI_DIR)
    set(_native_libs "${MOQ_FFI_DIR}/native-libs.txt")
else()
    set(_native_libs "${_workspace}/rs/moq-c/native-libs/${_platform}.txt")
endif()
file(STRINGS "${_native_libs}" _lines)
set(MOQ_NATIVE_LIBS "")
foreach(_line IN LISTS _lines)
    string(STRIP "${_line}" _line)
    if(_line STREQUAL "" OR _line MATCHES "^#")
        continue()
    elseif(_line MATCHES "^framework:(.+)$")
        list(APPEND MOQ_NATIVE_LIBS "-framework ${CMAKE_MATCH_1}")
    else()
        list(APPEND MOQ_NATIVE_LIBS "${_line}")
    endif()
endforeach()

# What a C linker must add for the C++ implementation: the C++ compiler's runtime libraries
# that the C compiler does not already link, such as libstdc++ or libc++. Empty on MSVC, whose
# objects name their runtime themselves.
# Whatever the native libraries already name, such as OpenH264's C++ runtime, is not repeated.
set(MOQ_CXX_RUNTIME ${CMAKE_CXX_IMPLICIT_LINK_LIBRARIES})
foreach(_lib IN LISTS CMAKE_C_IMPLICIT_LINK_LIBRARIES MOQ_NATIVE_LIBS)
    list(REMOVE_ITEM MOQ_CXX_RUNTIME "${_lib}")
endforeach()
list(REMOVE_DUPLICATES MOQ_CXX_RUNTIME)

# The C API: the generated implementation plus src/, as one static library beside moq-ffi's.
# Named moq-c so an in-tree build can sit beside the C++ package's `moq-cpp` target.
add_library(moq-c STATIC
    src/shutdown.c
    "${_generated}/moq.cpp"
    "${_generated}/moq_c.cpp"
)
add_library(moq::c ALIAS moq-c)
add_dependencies(moq-c moq_c_ffi)
set_target_properties(moq-c PROPERTIES
    OUTPUT_NAME moq
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    # The Rust staticlib links the release CRT (/MD) in every configuration, so the C++ objects
    # built with it do too.
    MSVC_RUNTIME_LIBRARY MultiThreadedDLL
)
target_compile_features(moq-c PUBLIC c_std_11)
target_include_directories(moq-c PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>"
)
find_package(Threads REQUIRED)
target_link_libraries(moq-c PUBLIC "${MOQ_FFI_LIB}" ${MOQ_NATIVE_LIBS} ${MOQ_CXX_RUNTIME} Threads::Threads)

if(PROJECT_IS_TOP_LEVEL)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    # moq-c.pc locates the prefix relative to itself, so the archive can be unpacked anywhere.
    foreach(_dir CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR)
        if(IS_ABSOLUTE "${${_dir}}")
            message(FATAL_ERROR "${_dir} must be relative to the prefix, not ${${_dir}}")
        endif()
    endforeach()
    file(RELATIVE_PATH MOQ_PC_PREFIX "/prefix/${CMAKE_INSTALL_LIBDIR}/pkgconfig" "/prefix")
    string(REGEX REPLACE "/$" "" MOQ_PC_PREFIX "${MOQ_PC_PREFIX}")

    install(TARGETS moq-c ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(FILES include/moq/moq.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
    install(FILES "${_generated}/moq.h" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq/ffi)
    install(FILES "${MOQ_FFI_LIB}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(FILES MIGRATE.md DESTINATION ${CMAKE_INSTALL_DATADIR}/moq-c)

    # The config takes lists; pkg-config wants linker flags.
    get_filename_component(MOQ_FFI_LIB_NAME "${MOQ_FFI_LIB}" NAME)
    set(MOQ_C_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}moq${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(MOQ_LIBS_PC "")
    foreach(_lib IN LISTS MOQ_NATIVE_LIBS MOQ_CXX_RUNTIME)
        if(_lib MATCHES "^-" OR IS_ABSOLUTE "${_lib}")
            string(APPEND MOQ_LIBS_PC " ${_lib}")
        else()
            string(APPEND MOQ_LIBS_PC " -l${_lib}")
        endif()
    endforeach()

    configure_package_config_file(cmake/moq-c-config.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/moq-c-config.cmake"
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/moq-c
        PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR
    )
    write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/moq-c-config-version.cmake"
        COMPATIBILITY SameMinorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/moq-c-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/moq-c-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/moq-c
    )

    configure_file(cmake/moq-c.pc.in "${CMAKE_CURRENT_BINARY_DIR}/moq-c.pc" @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/moq-c.pc" DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endif()

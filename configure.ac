AC_PREREQ(2.60)
AC_INIT([protobuf-c-rpc], [0.1])
AC_CONFIG_SRCDIR([protobuf-c-rpc/protobuf-c-rpc-client.c])
AC_CONFIG_SRCDIR([protobuf-c-rpc/protobuf-c-rpc-server.c])
AC_CONFIG_SRCDIR([protobuf-c-rpc/protobuf-c-rpc-data-buffer.c])
AC_CONFIG_SRCDIR([protobuf-c-rpc/protobuf-c-rpc-dispatch.c])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign 1.11 -Wall -Wno-portability silent-rules subdir-objects])
AC_PROG_CC_STDC
AC_USE_SYSTEM_EXTENSIONS
AC_SYS_LARGEFILE
AC_CONFIG_MACRO_DIR([m4])
AM_SILENT_RULES([yes])
LT_INIT

AC_CONFIG_HEADERS(config.h)
AC_CONFIG_FILES([Makefile protobuf-c-rpc/libprotobuf-c-rpc.pc])

AC_CHECK_HEADERS(alloca.h)
AC_CHECK_HEADERS(io.h)
AC_CHECK_HEADERS(malloc.h)
AC_CHECK_HEADERS(sys/poll.h)
AC_CHECK_HEADERS(sys/select.h)
AC_CHECK_HEADERS(sys/uio.h)
AC_CHECK_HEADERS(unistd.h)
AC_CHECK_HEADERS(winsock.h)

AC_SEARCH_LIBS([hstrerror], [resolv], [], [AC_MSG_FAILURE([hstrerror was not found on your system])], [-lsocket -lnsl])
AC_SEARCH_LIBS([connect], [socket], [], [AC_MSG_FAILURE([connect was not found on your system])], [-lnsl])

PKG_PROG_PKG_CONFIG
if test -n "$PKG_CONFIG"; then
    # Horrible hack for systems where the pkg-config install directory is simply wrong!
    if $PKG_CONFIG --variable=pc_path pkg-config 2>/dev/null | grep -q /libdata/; then
        PKG_INSTALLDIR(['${prefix}/libdata/pkgconfig'])
    else
        PKG_INSTALLDIR
    fi
else
    AC_MSG_ERROR([pkg-config is required!])
fi

PKG_CHECK_MODULES([libprotobuf_c], [libprotobuf-c >= 1.0.1])

AC_ARG_VAR([PROTOC_C], [protobuf-c compiler command])
AC_PATH_PROG([PROTOC_C], [protoc-c], [],
	     [`$PKG_CONFIG --variable=exec_prefix libprotobuf-c`/bin:$PATH])
if test -z "$PROTOC_C"; then
  AC_MSG_ERROR([Please install the protobuf-c compiler from https://github.com/protobuf-c/protobuf-c/.])
fi

gl_VALGRIND_TESTS

MY_CODE_COVERAGE

AC_OUTPUT
AC_MSG_RESULT([
    $PACKAGE $VERSION

        CC:                     ${CC}
        CFLAGS:                 ${CFLAGS}
        LDFLAGS:                ${LDFLAGS}
        LIBS:                   ${LIBS}

        prefix:                 ${prefix}
        sysconfdir:             ${sysconfdir}
        libdir:                 ${libdir}
        includedir:             ${includedir}
        pkgconfigdir:           ${pkgconfigdir}
])

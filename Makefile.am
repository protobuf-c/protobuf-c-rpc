bin_PROGRAMS =
check_PROGRAMS =
noinst_PROGRAMS =
lib_LTLIBRARIES =
nobase_include_HEADERS =
pkgconfig_DATA =
BUILT_SOURCES =
TESTS =
CLEANFILES =
DISTCLEANFILES =
EXTRA_DIST =
ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

AM_CPPFLAGS = \
	-include $(top_builddir)/config.h \
	-I${top_builddir} \
	-I${top_srcdir}
AM_CFLAGS = ${my_CFLAGS}
AM_LDFLAGS =

# code coverage

AM_CFLAGS += ${CODE_COVERAGE_CFLAGS}
AM_LDFLAGS += ${CODE_COVERAGE_LDFLAGS}
CODE_COVERAGE_LCOV_OPTIONS = --no-external
CODE_COVERAGE_IGNORE_PATTERN = "$(abs_top_builddir)/t/*"
@CODE_COVERAGE_RULES@

#
# libprotobuf-c-rpc
#

LIBPROTOBUF_C_RPC_CURRENT=0
LIBPROTOBUF_C_RPC_REVISION=0
LIBPROTOBUF_C_RPC_AGE=0

lib_LTLIBRARIES += \
	protobuf-c-rpc/libprotobuf-c-rpc.la

nobase_include_HEADERS += \
	protobuf-c-rpc/protobuf-c-rpc-data-buffer.h \
	protobuf-c-rpc/protobuf-c-rpc-dispatch.h \
	protobuf-c-rpc/protobuf-c-rpc.h

protobuf_c_rpc_libprotobuf_c_rpc_la_SOURCES = \
	protobuf-c-rpc/gsklistmacros.h \
	protobuf-c-rpc/gskrbtreemacros.h \
	protobuf-c-rpc/protobuf-c-rpc-data-buffer.c \
	protobuf-c-rpc/protobuf-c-rpc-dispatch.c \
	protobuf-c-rpc/protobuf-c-rpc-client.c \
	protobuf-c-rpc/protobuf-c-rpc-server.c

protobuf_c_rpc_libprotobuf_c_rpc_la_LDFLAGS = $(AM_LDFLAGS) \
	-version-info $(LIBPROTOBUF_C_RPC_CURRENT):$(LIBPROTOBUF_C_RPC_REVISION):$(LIBPROTOBUF_C_RPC_AGE) \
	-export-symbols-regex "^(protobuf_c_rpc_[a-z].*)"

pkgconfig_DATA += protobuf-c-rpc/libprotobuf-c-rpc.pc
CLEANFILES += protobuf-c-rpc/libprotobuf-c-rpc.pc
EXTRA_DIST += protobuf-c-rpc/libprotobuf-c-rpc.pc.in

#
# protobuf-c-rpc tests
#

LOG_COMPILER = $(VALGRIND)

check_PROGRAMS += \
	t/test-rpc

TESTS += \
	t/test-rpc

t_test_rpc_SOURCES = \
	t/test-rpc.c \
	t/test.pb-c.c
t_test_rpc_LDADD = \
	$(libprotobuf_c_LIBS) \
	protobuf-c-rpc/libprotobuf-c-rpc.la

t/test.pb-c.c t/test.pb-c.h: $(top_srcdir)/t/test.proto
	$(AM_V_GEN)@PROTOC_C@ -I$(top_srcdir) --c_out=$(top_builddir) $(top_srcdir)/t/test.proto

BUILT_SOURCES += \
	t/test.pb-c.c t/test.pb-c.h

EXTRA_DIST += \
	t/test.proto

#
#
#

CLEANFILES += $(BUILT_SOURCES)

dist-hook:
	rm -vf `find $(top_distdir) -name '*.pb-c.[ch]' -o -name '*.pb.cc' -o -name '*.pb.h'`

## LibKafkaConect
## A C++ shared libray for connecting to Kafka

#
# Warning this is the first time I've made a configure.ac/Makefile.am thing
# Please improve it as I have no idea what I am doing
# @benjamg
#

ACLOCAL_AMFLAGS = -I build-aux/m4 ${ACLOCAL_FLAGS}
AM_CPPFLAGS = $(DEPS_CFLAGS)
EXAMPLE_LIBS = -lboost_system -lboost_thread -lkafkaconnect

#
# Shared Library
#

lib_LTLIBRARIES = libkafkaconnect.la

libkafkaconnect_la_SOURCES = src/producer.cpp
libkafkaconnect_la_LDFLAGS = -version-info $(KAFKACONNECT_VERSION)

kafkaconnect_includedir = $(includedir)/kafkaconnect
kafkaconnect_include_HEADERS = src/producer.hpp \
	src/encoder.hpp \
	src/encoder_helper.hpp


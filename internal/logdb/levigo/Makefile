include build_flags/linux.mk
linux_sources := $(sort $(SOURCES))

include build_flags/darwin.mk
darwin_sources := $(sort $(SOURCES))

# for shell substitution
SHELL := /bin/bash

# Take the set union of the source files for all the supported platforms
all_sources := $(sort $(linux_sources) $(darwin_sources))

link_leveldb:
	@for sf in $(all_sources); do \
		f="deps/leveldb/$$sf"; \
		rm -f $${f//\//_}; \
		ln -s $$f $${f//\//_}; \
	done

link_snappy:
	rm -f deps_snappy*
	ln -s deps/snappy/snappy.cc deps_snappy.cc
	ln -s deps/snappy/snappy-sinksource.cc deps_snappy-sinksource.cc

link_lz4:
	rm -f deps_lz4*
	ln -s deps/lz4/lz4.c deps_lz4.c

link: link_snappy link_leveldb link_lz4

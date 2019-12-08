
export RUSTFLAGS=-C target-cpu=native

build_dir = target/release

detected_OS = $(shell uname)
ifeq ($(detected_OS), Linux)
	target_dir = ~/usr/bin/
else
	target_dir = ~/bin/
endif

define build_prog
	strip $(build_dir)/$(1) && cp $(build_dir)/$(1) $(target_dir)
endef

install:
	cargo build --release
	$(call build_prog,bufcli)


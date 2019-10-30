
export RUSTFLAGS=-C target-cpu=native

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
	install_dir = ~/usr/bin/
else 
	install_dir = ~/bin/
endif

target_dir = target/release

define build_prog
	strip $(target_dir)/$(1) && cp $(target_dir)/$(1) $(install_dir)
endef

install:
	cargo build --release
	$(call build_prog,bufcli)


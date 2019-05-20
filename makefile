
export RUSTFLAGS=-C target-cpu=native

target_dir = target/release

define build_prog
	strip $(target_dir)/$(1) && cp $(target_dir)/$(1) ~/usr/bin/
endef

install:
	cargo build --release
	$(call build_prog,bufcli)
	
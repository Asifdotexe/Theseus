# Cross-platform Makefile for Theseus Engine and Data Pipeline

.PHONY: all build run test clean pipeline

all: build

build:
	cargo build --release --manifest-path engine/Cargo.toml

run:
	cargo run --release --manifest-path engine/Cargo.toml -- $(ARGS)

test:
	cargo test --manifest-path engine/Cargo.toml

clean:
	cargo clean --manifest-path engine/Cargo.toml

pipeline:
	python -m scripts.run_pipeline $(ARGS)

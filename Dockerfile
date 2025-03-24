# 🌍 Use latest Rust version
FROM rust:latest AS builder

# ✅ Install protobuf compiler
# ✅ Install protobuf + build deps for rdkafka
RUN apt-get update && \
    apt-get install -y \
    protobuf-compiler \
    cmake \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    zlib1g-dev \
    curl \
    build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


WORKDIR /usr/src/insightsx

# ✅ Copy Cargo files first for dependency caching
COPY Cargo.toml Cargo.lock ./

# ✅ Create dummy src structure for dependency compilation
RUN mkdir -p src/bin
RUN echo "fn main() {}" > src/main.rs
RUN echo "fn main() {}" > src/bin/log_consumer.rs
RUN echo "fn main() {}" > src/bin/benchmark.rs  # <-- Ensures this dummy file exists

# ✅ Build dependencies only (does NOT include real source code yet)
RUN cargo build --release

# ✅ Now copy the REAL source code (including `benchmark.rs`)
COPY . .

# ✅ Explicitly build the actual binaries (including `benchmark`)
RUN cargo build --release --bin insightsx
RUN cargo build --release --bin log_consumer
RUN cargo build --release --bin benchmark  # <-- Ensures benchmark is built

# ✅ Deploy minimal runtime image
FROM debian:bookworm-slim
WORKDIR /usr/local/bin

# ✅ Install required runtime dependencies
RUN apt-get update && \
    apt-get install -y libssl-dev ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/insightsx/target/release/insightsx .
COPY --from=builder /usr/src/insightsx/target/release/log_consumer .
COPY --from=builder /usr/src/insightsx/target/release/benchmark .  

CMD ["./insightsx"]

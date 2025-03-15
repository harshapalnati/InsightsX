# Stage 1: Build the Rust application
FROM rust:latest AS builder

# Set working directory
WORKDIR /usr/src/insightsx

# Install required dependencies
RUN apt-get update && apt-get install -y protobuf-compiler

# Cache dependencies clearly
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# Copy project files
COPY . .

# Build application (optimized)
RUN cargo build --release --bin insightsx

# Stage 2: Create minimal final runtime image
FROM debian:bookworm-slim

WORKDIR /usr/local/bin

# Install required dependencies (if necessary, e.g., for TLS, networking)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy compiled binary
COPY --from=builder /usr/src/insightsx/target/release/insightsx .

# Expose port (update based on your configuration)
EXPOSE 3000

# Run binary
CMD ["./insightsx"]

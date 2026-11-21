# Shopping Sessions: hand-written Rust pipeline. linux/amd64 on the scorer.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release && strip target/release/shopping

FROM gcr.io/distroless/cc-debian12
COPY --from=build /src/target/release/shopping /app/shopping
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch
CMD ["/app/shopping"]

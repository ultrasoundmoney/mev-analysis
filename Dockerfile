FROM lukemathwalker/cargo-chef:latest-rust-bullseye AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
# Figure out if dependencies have changed.
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this layer is cached for massive speed up.
RUN cargo chef cook --release --recipe-path recipe.json
# Build application - this should be re-done every time we update our src.
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim AS runtime
WORKDIR /app

# sqlx depends on native TLS, which is missing in buster-slim.
RUN apt update && apt install -y libssl1.1 ca-certificates

COPY --from=builder /app/target/release/relay-backend /usr/local/bin
COPY --from=builder /app/target/release/phoenix-service /usr/local/bin
COPY --from=builder /app/target/release/serve /usr/local/bin
COPY --from=builder /app/target/release/ingest-block-production /usr/local/bin

EXPOSE 3002
ENTRYPOINT ["/usr/local/bin/relay-backend"]

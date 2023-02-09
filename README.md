# mev-analysis

Various code used to run [relay.ultrasound.money](https://relay.ultrasound.money).

Requires the following env vars (or subset depending on if `--bin` is `serve` or `phoenix-service`):

```bash
# mev-boost-relay dependencies
DATABASE_URL=postgres://user:pw@localhost:5432/dbname
CONSENSUS_NODES=http://X.X.X.X:3500,http://Y.Y.Y.Y:3500
VALIDATION_NODES=http://X.X.X.X:8545,http://Y.Y.Y.Y:8545

# other
ENV=prod
PORT=3002
RUST_LOG=info
REDIS_URI=localhost:6379
OPSGENIE_API_KEY=XXX
```

Build:
```bash
cargo build
```

Run:
```bash
export $(cat .env | xargs) && cargo run --bin serve
```

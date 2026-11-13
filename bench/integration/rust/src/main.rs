//! The tonic arm of the local integration race: bench/integration/README.md.
//!
//! It serves `bench.v1.Bench/Unary` with a constant reply and never looks at
//! the request, so a round measures transport and framing and nothing else.
//! The service stub is generated from ../bench.proto at build time; the
//! handler is the whole arm.
//!
//! Usage: bench-integration-rust [port] [reply-size], defaults 50153 and 8.

mod pb {
    include!(concat!(env!("OUT_DIR"), "/bench.v1.rs"));
}

use std::sync::OnceLock;

use bytes::Bytes;
use tonic::{transport::Server, Request, Response, Status};

/// The same stream limit every arm advertises (README, "Configuration
/// parity"). Above every profile in the matrix, so no arm throttles.
const MAX_STREAMS: usize = 1000;

static REPLY: OnceLock<Bytes> = OnceLock::new();

pub struct Bench;

#[tonic::async_trait]
impl pb::bench_server::Bench for Bench {
    async fn unary(
        &self,
        _request: Request<pb::Payload>,
    ) -> Result<Response<pb::Payload>, Status> {
        Ok(Response::new(pb::Payload {
            data: REPLY.get().cloned().unwrap_or_default(),
        }))
    }
}

/// The `Payload.data` bytes the arm returns: the handler wraps them in the
/// generated `Payload`, so unlike the zig and go arms — whose replies are
/// precomputed wire bytes under raw codecs — this is the bare payload, not a
/// serialized message. The first version of this arm double-wrapped, and the
/// loadgen's content check caught it on its first run.
fn compose(size: usize) -> Bytes {
    let mut out = Vec::with_capacity(size);
    out.extend(std::iter::repeat_n(b'a', size));
    Bytes::from(out)
}

#[tokio::main]
async fn main() {
    let mut port: u16 = 50153;
    let mut size: usize = 8;
    let args: Vec<String> = std::env::args().collect();
    if let Some(arg) = args.get(1) {
        if let Ok(parsed) = arg.parse() {
            port = parsed;
        }
    }
    if let Some(arg) = args.get(2) {
        if let Ok(parsed) = arg.parse() {
            size = parsed;
        }
    }
    let _ = REPLY.set(compose(size));

    let addr = format!("127.0.0.1:{port}").parse().expect("valid address");
    println!("rust arm listening on 127.0.0.1:{port}, {size} byte replies");

    Server::builder()
        .concurrency_limit_per_connection(MAX_STREAMS)
        .add_service(pb::bench_server::BenchServer::new(Bench))
        .serve(addr)
        .await
        .expect("serve");
}

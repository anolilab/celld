//! The response bodies `fetch()` decodes for the Worker.

use crate::engine_api::HttpChunkStream;
use futures_util::StreamExt as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Decoded bytes read before the expansion check starts. A small body may
/// legitimately expand far more than the limit.
const FREE_DECODED_BYTES: u64 = 1024 * 1024;

/// The most a body may expand: decoded bytes per encoded byte read, once
/// `FREE_DECODED_BYTES` are decoded. A body past it ends with an error, so a
/// few encoded bytes cannot make celld produce gigabytes.
const MAX_EXPANSION: u64 = 1024;

/// The size of each decoded chunk handed to the Worker.
const DECODED_CHUNK_BYTES: usize = 64 * 1024;

/// A response `Content-Encoding` that `fetch()` decodes for the Worker: a
/// single `gzip` or `br` value, matched exactly. Any other value, including a
/// list of codings, reaches the Worker as sent.
#[derive(Clone, Copy)]
pub(crate) enum ResponseEncoding {
    Gzip,
    Brotli,
}

impl ResponseEncoding {
    /// The coding to decode, if any. A response to `HEAD`, a `204` and a `304`
    /// have no body to decode, and keep their headers.
    pub(crate) fn of(
        method: &reqwest::Method,
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
    ) -> Option<Self> {
        if method == reqwest::Method::HEAD
            || status == reqwest::StatusCode::NO_CONTENT
            || status == reqwest::StatusCode::NOT_MODIFIED
        {
            return None;
        }
        let mut values = headers.get_all(reqwest::header::CONTENT_ENCODING).iter();
        match (values.next()?.as_bytes(), values.next()) {
            (b"gzip", None) => Some(Self::Gzip),
            (b"br", None) => Some(Self::Brotli),
            _ => None,
        }
    }

    /// The decoded body, read from the connection as the Worker pulls it.
    pub(crate) fn decode(self, response: reqwest::Response) -> HttpChunkStream {
        use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder};
        let encoded = Arc::new(AtomicU64::new(0));
        let counted = encoded.clone();
        let body = tokio_util::io::StreamReader::new(response.bytes_stream().map(move |chunk| {
            chunk
                .inspect(|bytes| {
                    counted.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                })
                .map_err(std::io::Error::other)
        }));
        let decoded: Pin<Box<dyn tokio::io::AsyncRead + Send>> = match self {
            Self::Gzip => {
                let mut decoder = GzipDecoder::new(body);
                // A gzip body may be several members back to back (RFC 1952).
                decoder.multiple_members(true);
                Box::pin(decoder)
            }
            Self::Brotli => Box::pin(BrotliDecoder::new(body)),
        };
        Box::pin(
            tokio_util::io::ReaderStream::with_capacity(decoded, DECODED_CHUNK_BYTES).scan(
                (0_u64, false),
                move |(total, finished), chunk| {
                    let item = if *finished {
                        None
                    } else {
                        match chunk {
                            Ok(bytes) => {
                                *total += bytes.len() as u64;
                                if *total > FREE_DECODED_BYTES
                                    && *total > MAX_EXPANSION * encoded.load(Ordering::Relaxed)
                                {
                                    *finished = true;
                                    Some(Err(format!(
                                        "response stream: the decoded body is more than \
                                         {MAX_EXPANSION} times the size of the encoded body"
                                    )))
                                } else {
                                    Some(Ok(bytes.to_vec()))
                                }
                            }
                            // A body with no bytes at all has nothing to
                            // decode, and reads as empty.
                            Err(_) if encoded.load(Ordering::Relaxed) == 0 => None,
                            Err(error) => {
                                *finished = true;
                                Some(Err(format!("response stream: {error}")))
                            }
                        }
                    };
                    std::future::ready(item)
                },
            ),
        )
    }
}

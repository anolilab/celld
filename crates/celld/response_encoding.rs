//! The response bodies `fetch()` decodes for the Worker.

use crate::engine_api::HttpChunkStream;
use futures_util::StreamExt as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
/// single `gzip` or `br` value, matched without regard to case (RFC 9110). Any
/// other value, including a list of codings, reaches the Worker as sent.
#[derive(Clone, Copy)]
pub(crate) enum ResponseEncoding {
    Gzip,
    Brotli,
}

impl ResponseEncoding {
    /// The coding to decode, if any. A response to `HEAD`, an informational
    /// status, a `204`, a `205` and a `304` have no body to decode, and keep
    /// their headers.
    pub(crate) fn of(
        method: &reqwest::Method,
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
    ) -> Option<Self> {
        if method == reqwest::Method::HEAD
            || status.is_informational()
            || status == reqwest::StatusCode::NO_CONTENT
            || status == reqwest::StatusCode::RESET_CONTENT
            || status == reqwest::StatusCode::NOT_MODIFIED
        {
            return None;
        }
        let mut values = headers.get_all(reqwest::header::CONTENT_ENCODING).iter();
        let (first, second) = (values.next()?.as_bytes(), values.next());
        match second {
            None if first.eq_ignore_ascii_case(b"gzip") => Some(Self::Gzip),
            None if first.eq_ignore_ascii_case(b"br") => Some(Self::Brotli),
            _ => None,
        }
    }

    /// The decoded body, read from the connection as the Worker pulls it.
    pub(crate) fn decode(self, response: reqwest::Response) -> HttpChunkStream {
        use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder};
        let encoded = Arc::new(AtomicU64::new(0));
        let counted = encoded.clone();
        // Set when the connection fails, to tell that from a body that is
        // empty: both leave no encoded bytes behind.
        let broken = Arc::new(AtomicBool::new(false));
        let failed = broken.clone();
        let body = tokio_util::io::StreamReader::new(response.bytes_stream().map(move |chunk| {
            chunk
                .inspect(|bytes| {
                    counted.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                })
                .map_err(|error| {
                    failed.store(true, Ordering::Relaxed);
                    std::io::Error::other(error)
                })
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
                            // decode, and reads as empty. A connection that
                            // failed before the first byte is an error.
                            Err(_)
                                if encoded.load(Ordering::Relaxed) == 0
                                    && !broken.load(Ordering::Relaxed) =>
                            {
                                None
                            }
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

//! A plain-HTTP proxy in front of an S3-compatible store that injects
//! faults into the requests it relays.
//!
//! Requests are forwarded unchanged, `Host` header included, so a SigV4
//! signature made for the proxy's address still verifies upstream. Every
//! request is recorded by method, faulted or not.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Body = BoxBody<Bytes, hyper::Error>;

#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// Answer with this status without forwarding.
    Status(StatusCode),
    /// Never answer; the client's own timeout ends the request.
    Stall,
    /// Close the connection without forwarding.
    Reset,
    /// Forward, wait for the store's answer, then close the connection
    /// without passing it on: the store applied a request whose client
    /// never learns the outcome.
    DropResponse,
}

#[derive(Default)]
struct Plan {
    /// One-shot faults, each taken by the first request with its method.
    once: VecDeque<(Method, Fault)>,
    /// Applied to every request that no one-shot fault claims.
    always: Option<Fault>,
    seen: Vec<Method>,
}

impl Plan {
    fn take(&mut self, method: &Method) -> Option<Fault> {
        self.seen.push(method.clone());
        match self.once.iter().position(|(m, _)| m == method) {
            Some(at) => self.once.remove(at).map(|(_, fault)| fault),
            None => self.always,
        }
    }
}

pub struct Proxy {
    url: String,
    plan: Arc<Mutex<Plan>>,
    accept: JoinHandle<()>,
}

impl Proxy {
    /// Listens on a free loopback port and relays to `upstream`
    /// (`host:port`).
    pub async fn start(upstream: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let plan = Arc::new(Mutex::new(Plan::default()));
        let shared = plan.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (plan, upstream) = (shared.clone(), upstream.clone());
                let service =
                    service_fn(move |request| relay(plan.clone(), upstream.clone(), request));
                tokio::spawn(
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service),
                );
            }
        });
        Proxy { url, plan, accept }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Faults the next request with `method`.
    pub fn once(&self, method: Method, fault: Fault) {
        self.plan.lock().unwrap().once.push_back((method, fault));
    }

    /// Faults every request from now on.
    pub fn always(&self, fault: Fault) {
        self.plan.lock().unwrap().always = Some(fault);
    }

    /// How many requests with `method` reached the proxy.
    pub fn seen(&self, method: &Method) -> usize {
        let plan = self.plan.lock().unwrap();
        plan.seen.iter().filter(|m| *m == method).count()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn relay(
    plan: Arc<Mutex<Plan>>,
    upstream: String,
    request: Request<Incoming>,
) -> Result<Response<Body>, BoxError> {
    let fault = plan.lock().unwrap().take(request.method());
    match fault {
        Some(Fault::Status(status)) => {
            let body = Full::new(Bytes::from_static(b"injected fault"))
                .map_err(|never| match never {})
                .boxed();
            Ok(Response::builder().status(status).body(body)?)
        }
        Some(Fault::Stall) => std::future::pending().await,
        Some(Fault::Reset) => Err("injected reset".into()),
        Some(Fault::DropResponse) => {
            let response = forward(&upstream, request).await?;
            response.into_body().collect().await?;
            Err("injected lost response".into())
        }
        None => Ok(forward(&upstream, request).await?.map(BodyExt::boxed)),
    }
}

async fn forward(
    upstream: &str,
    request: Request<Incoming>,
) -> Result<Response<Incoming>, BoxError> {
    let stream = TcpStream::connect(upstream).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(connection);
    Ok(sender.send_request(request).await?)
}

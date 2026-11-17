// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Reaching a reserved-class cell from outside the fleet.
//!
//! The operator commands have to find a live node, sign a request with
//! the fleet secret, and post it to `/runtime/<scope>`, which forwards to the
//! cell's owner. Neither can open the object in the bucket instead: that writes
//! behind the fence, and the owner's next flush overwrites it. Neither can name
//! one node either, because an operator's internal listener has an ephemeral
//! port by default. Both constraints are `d1.md` decision 4's, and neither
//! becomes less true for the second command that meets them.
//!
//! So this is that machinery, once. It was `d1_cli`'s, and it was already
//! generic in everything but the noun in its messages and the label it signs
//! with -- which is the shape that gets copied rather than shared. What stays
//! in `d1_cli` is what is actually about SQL: `exec`, `query`, and the
//! migration ledger, which extend `Reachable` from there.

use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use serde_json::Value;

use crate::bucket::Bucket;

/// What a command is reaching, for the operator reading the error.
pub(crate) struct Subject {
    /// The resource as an operator names it: "database", "namespace".
    pub(crate) noun: &'static str,
    /// The `peer_auth` source label, which is what appears in a node's log.
    pub(crate) source: &'static str,
    /// How long one call may take. A migration on a large table is not a
    /// diagnostic ping, so this is the handler budget's order and not the
    /// probe's.
    pub(crate) timeout: Duration,
}

/// Where the fleet is, and how much the caller trusts what it advertises.
pub(crate) struct Fleet<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) endpoint: Option<&'a str>,
    pub(crate) region: &'a str,
    pub(crate) unsafe_public_advertise: bool,
}

/// Which node addresses a client may send signed requests to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Addresses {
    /// Private and overlay addresses; a public one is refused.
    Private,
    /// Any address, for an operator who passed `--unsafe-public-advertise`.
    Public,
    /// Only this host: a lease read from a local store may have been written
    /// by someone else, and the request carries credentials.
    Loopback,
}

impl Addresses {
    pub(crate) fn fleet(unsafe_public_advertise: bool) -> Self {
        if unsafe_public_advertise {
            Self::Public
        } else {
            Self::Private
        }
    }

    /// Why a lease's address may not be used, or nothing when it may.
    fn refusal(self, advertise: &crate::startup::Advertise, addr: &str) -> Option<String> {
        match self {
            Self::Loopback if !advertise.is_loopback() => Some(format!(
                "{addr} is not a loopback address; --local only reaches a node on this host"
            )),
            Self::Private if advertise.is_public_ip() => Some(format!(
                "public advertise address {addr}; use a private overlay or \
                 --unsafe-public-advertise"
            )),
            _ => None,
        }
    }
}

pub(crate) struct Entrance {
    node: String,
    url: String,
    path: String,
}

/// A database resolved to the nodes that can carry SQL to it.
pub(crate) struct Reachable {
    http: reqwest::Client,
    /// The fleet's shared peer secret, read from the bucket. `/runtime/` refuses
    /// an unsigned request, because a D1 database holds application data and
    /// answers arbitrary SQL.
    auth: crate::peer_auth::PeerAuth,
    /// Every node with a live lease. Any one of them forwards to the owner, so
    /// the list is a set of equal entrances and not a route: it stays correct
    /// even when the cell moves between calls.
    entrances: Vec<Entrance>,
    subject: Subject,
    /// What to say when no entrance accepts a connection, for a caller that
    /// knows why (a node it expects on this host that is not running).
    unreachable: Option<String>,
}

impl Reachable {
    pub(crate) async fn open(
        fleet: Fleet<'_>,
        subject: Subject,
        scope: String,
        name: Option<&str>,
    ) -> anyhow::Result<Self> {
        let bucket = crate::fleet::bucket_client(fleet.bucket, fleet.endpoint, fleet.region)?;
        let mut path = format!("/runtime/{scope}");
        if let Some(name) = name {
            // An operator command can be the first request to construct a
            // reserved cell. A later named binding call cannot repair the
            // identity that the existing JavaScript instance already cached.
            path.push('?');
            path.push_str(
                &url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("name", name)
                    .finish(),
            );
        }
        Self::open_at(
            &bucket,
            Addresses::fleet(fleet.unsafe_public_advertise),
            subject,
            path,
            None,
        )
        .await
    }

    /// Reach a route of the internal listener on any live node of the fleet in
    /// `bucket`. `auth` replaces the credentials read from the bucket and the
    /// environment, for a caller that holds its own.
    pub(crate) async fn open_at(
        bucket: &Bucket,
        addresses: Addresses,
        subject: Subject,
        path: String,
        auth: Option<crate::peer_auth::PeerAuth>,
    ) -> anyhow::Result<Self> {
        let nodes = live_nodes(bucket, addresses, subject.source).await?;
        // The same secret and the same source shape `celld diagnose` uses.
        let auth = match auth {
            Some(auth) => auth,
            None => crate::peer_auth::PeerAuth::for_client(
                crate::peer_auth::load_existing(bucket).await?,
                subject.source,
            )?,
        };
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            // A migration on a large table is not a diagnostic ping, so this
            // budget is the handler budget's order rather than the probe's.
            .timeout(subject.timeout)
            .build()
            .with_context(|| format!("build the {} client", subject.noun))?;
        Ok(Self {
            http,
            auth,
            subject,
            entrances: nodes
                .into_iter()
                .map(|(node, addr)| Entrance {
                    node,
                    url: format!("http://{addr}{path}"),
                    path: path.clone(),
                })
                .collect(),
            unreachable: None,
        })
    }

    /// Answer a connection refused by every entrance with `message` rather
    /// than the transport error.
    pub(crate) fn when_unreachable(mut self, message: String) -> Self {
        self.unreachable = Some(message);
        self
    }

    /// A lease outlives the node that wrote it, and a node that drains keeps
    /// its lease while it refuses work, so the first entrance in the list is
    /// not always usable. Move to the next one when a node cannot be reached
    /// or is draining, because in both cases the request provably did not run.
    /// Nothing else is retried: a timeout is not evidence that the work was
    /// refused, and re-sending a migration that in fact ran would apply it
    /// twice.
    pub(crate) async fn call(&self, body: Value) -> anyhow::Result<Value> {
        let (status, text, refused) = self.exchange(body).await?;
        let value: Value = serde_json::from_str(&text)
            .with_context(|| format!("decode the reply ({status}): {text}"))?;
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            bail!("{error}{}", report(&refused));
        }
        if !status.is_success() {
            bail!(
                "the {} refused the request ({status}): {text}{}",
                self.subject.noun,
                report(&refused)
            );
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("the reply carried no result: {text}{}", report(&refused)))
    }

    /// Like [`Self::call`] for a request whose answer is text and not JSON,
    /// such as a SQL dump. A refusal is still a JSON `{"error": ...}`.
    pub(crate) async fn call_text(&self, body: Value) -> anyhow::Result<String> {
        let (status, text, refused) = self.exchange(body).await?;
        if status.is_success() {
            return Ok(text);
        }
        let message = serde_json::from_str::<Value>(&text).ok().and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
        match message {
            Some(error) => bail!("{error}{}", report(&refused)),
            None => bail!(
                "the {} refused the request ({status}): {text}{}",
                self.subject.noun,
                report(&refused)
            ),
        }
    }

    /// Send one signed request to the first entrance that takes it, and
    /// return the status and body of its answer.
    async fn exchange(
        &self,
        body: Value,
    ) -> anyhow::Result<(reqwest::StatusCode, String, Vec<String>)> {
        // Signed per entrance, because the signature binds the node it is
        // addressed to: a signature for one node is not replayable at another.
        let payload = serde_json::to_vec(&body).context("encode the request")?;
        let mut refused = Vec::new();
        for (index, entrance) in self.entrances.iter().enumerate() {
            let last = index + 1 == self.entrances.len();
            let url = &entrance.url;
            let request = self
                .auth
                .sign(
                    self.http.post(url).body(payload.clone()),
                    "POST",
                    &entrance.path,
                    &payload,
                    &entrance.node,
                )
                .with_context(|| format!("sign the request to {}", entrance.node))?;
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) if error.is_connect() && !last => {
                    refused.push(format!("{url}: {error}"));
                    continue;
                }
                Err(error) if error.is_connect() && self.unreachable.is_some() => {
                    refused.push(format!("{url}: {error}"));
                    bail!(
                        "{}{}",
                        self.unreachable.as_deref().unwrap_or_default(),
                        report(&refused)
                    );
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)).with_context(|| {
                        format!(
                            "reach the {} at {url}{}",
                            self.subject.noun,
                            report(&refused)
                        )
                    });
                }
            };
            let status = response.status();
            let text = response.text().await.context("read the reply")?;
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && text.contains("\"draining\"")
                && !last
            {
                refused.push(format!("{url}: the node is draining"));
                continue;
            }
            // The dispatcher refusing at the door is the doc comment's
            // criterion exactly: the request provably did not run, so the
            // next entrance is safe to try where a timeout would not be.
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && text.contains("dispatcher unavailable")
                && !last
            {
                refused.push(format!("{url}: the node's dispatcher is unavailable"));
                continue;
            }
            return Ok((status, text, refused));
        }
        bail!("no node in the fleet answered{}", report(&refused))
    }
}

async fn live_nodes(
    bucket: &Bucket,
    addresses: Addresses,
    source: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let nodes = crate::fleet::node_lease_ids(bucket).await?;
    if nodes.is_empty() {
        bail!("no node leases in the bucket; celld {source} needs a running fleet");
    }
    let mut reachable = Vec::new();
    let mut refused = Vec::new();
    for node in &nodes {
        let lease = match crate::fleet::live_node_lease(bucket, node).await {
            Ok(Some(lease)) => lease,
            Ok(None) => continue,
            Err(error) => {
                refused.push(format!("{node}: {error}"));
                continue;
            }
        };
        let advertise = match crate::startup::parse_advertise(&lease.addr) {
            Ok(advertise) => advertise,
            Err(error) => {
                refused.push(format!(
                    "{node}: malformed address {:?}: {error}",
                    lease.addr
                ));
                continue;
            }
        };
        if let Some(why) = addresses.refusal(&advertise, &lease.addr) {
            refused.push(format!("{node}: {why}"));
            continue;
        }
        reachable.push((node.clone(), lease.addr.clone()));
    }
    if reachable.is_empty() {
        bail!(
            "no node in the fleet is reachable; every lease is expired or refused{}",
            report(&refused)
        );
    }
    Ok(reachable)
}

/// Indented detail under a refusal, or nothing when there is none to give.
pub(crate) fn report(refused: &[String]) -> String {
    if refused.is_empty() {
        String::new()
    } else {
        format!("\n  {}", refused.join("\n  "))
    }
}

#[cfg(test)]
mod operator_token_tests {
    use super::*;
    use crate::operator_token::OperatorToken;

    #[tokio::test]
    async fn an_operator_call_carries_the_operator_token() {
        let stub = crate::test_stub::Recorder::start().await;
        let token = OperatorToken::parse(std::ffi::OsStr::new("tok-6")).unwrap();
        let auth = crate::peer_auth::PeerAuth::new([6; 32], "d1-cli")
            .unwrap()
            .with_operator_token(Some(token));
        let path = "/runtime/scope".to_string();
        let reachable = Reachable {
            http: reqwest::Client::new(),
            auth,
            entrances: vec![Entrance {
                node: "node-b".to_string(),
                url: format!("http://{}{path}", stub.addr()),
                path,
            }],
            subject: Subject {
                noun: "database",
                source: "d1-cli",
                timeout: Duration::from_secs(5),
            },
            unreachable: None,
        };
        let _ = reachable.call(serde_json::json!({})).await;
        assert_eq!(
            stub.authorizations(),
            vec![Some("Bearer tok-6".to_string())]
        );
    }
}

#[cfg(test)]
mod address_tests {
    use super::Addresses;
    use crate::startup::parse_advertise;

    fn refused(addresses: Addresses, addr: &str) -> bool {
        addresses
            .refusal(&parse_advertise(addr).unwrap(), addr)
            .is_some()
    }

    #[test]
    fn a_local_client_only_reaches_this_host() {
        for addr in ["127.0.0.1:9001", "[::1]:9001", "localhost:9001"] {
            assert!(!refused(Addresses::Loopback, addr), "{addr}");
        }
        for addr in ["10.0.0.5:9001", "8.8.8.8:9001", "node.internal:9001"] {
            assert!(refused(Addresses::Loopback, addr), "{addr}");
        }
    }

    #[test]
    fn a_fleet_client_refuses_a_public_address_unless_allowed() {
        assert!(!refused(Addresses::Private, "10.0.0.5:9001"));
        assert!(refused(Addresses::Private, "8.8.8.8:9001"));
        assert!(!refused(Addresses::Public, "8.8.8.8:9001"));
    }
}

#[cfg(test)]
mod unreachable_tests {
    use super::*;

    #[tokio::test]
    async fn a_refused_connection_gets_the_callers_message() {
        // A port nobody listens on: bind, note the port, drop the listener.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let path = "/runtime/scope".to_string();
        let reachable = Reachable {
            http: reqwest::Client::new(),
            auth: crate::peer_auth::PeerAuth::new([6; 32], "d1-cli").unwrap(),
            entrances: vec![Entrance {
                node: "node-b".to_string(),
                url: format!("http://127.0.0.1:{port}{path}"),
                path,
            }],
            subject: Subject {
                noun: "database",
                source: "d1-cli",
                timeout: Duration::from_secs(5),
            },
            unreachable: None,
        }
        .when_unreachable("start `celld dev` first".to_string());
        let error = reachable.call(serde_json::json!({})).await.unwrap_err();
        assert!(
            error.to_string().starts_with("start `celld dev` first"),
            "{error}"
        );
        assert!(
            error.to_string().contains(&format!("127.0.0.1:{port}")),
            "the refused address is named: {error}"
        );
    }
}

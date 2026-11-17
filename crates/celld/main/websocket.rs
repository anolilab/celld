// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! WebSockets on the ingress: accepting them, proxying them to the owner,
//! and the tasks that pump each one.
//!
//! A socket outlives the request that opened it, so each becomes its own
//! task. Three shapes exist — a local socket to a cell on this node, a
//! socket proxied to the owning peer, and a socket the worker opened
//! outbound — and they differ only in what sits on the far end.
use super::*;

/// Where the events of one socket go.
///
/// Resolved once, when the socket's task starts, and passed to every event of
/// the socket. The core's cancel path closes a socket and unregisters it
/// before the socket task dispatches the close, so a route read again at
/// that point would find no facet record and name the facet's scope as a
/// cell. Routing a request to that scope starts an activation for a cell
/// that no Worker exports.
#[derive(Clone)]
enum SocketRoute {
    /// A socket that a cell accepted.
    Cell(String),
    /// A socket that a Durable Object facet accepted. Routing, activity, and
    /// the output gate belong to the root cell. Only the handler runs in
    /// the facet.
    Facet {
        scope: String,
        facet: celld::js::FacetSocket,
    },
}

impl SocketRoute {
    /// Read the route of `target` while it is still registered.
    fn of(target: &celld::js::WsTarget) -> Self {
        match celld::js::ws_facet(target.id) {
            Some(facet) => Self::Facet {
                scope: target.scope.clone(),
                facet,
            },
            None => Self::Cell(target.scope.clone()),
        }
    }

    /// The scope that accepted the socket: the cell, or the facet.
    fn scope(&self) -> &str {
        match self {
            Self::Cell(cell) => cell,
            Self::Facet { scope, .. } => scope,
        }
    }

    /// The cell that owns the socket.
    fn cell(&self) -> &str {
        match self {
            Self::Cell(cell) => cell,
            Self::Facet { facet, .. } => &facet.root,
        }
    }
}

async fn dispatch_ws_message(
    app: &AppHandle,
    socket: &SocketRoute,
    ws_id: u64,
    data: celld::js::WsIn,
) -> anyhow::Result<()> {
    let scope = socket.scope();
    // The auto-response short circuit: a matched text frame is answered here
    // in the shell and never becomes a `webSocketMessage`. No routing, no
    // activity, no wake — a hibernated cell stays hibernated, which is the
    // feature.
    if let celld::js::WsIn::Text(text) = &data {
        if let Some(response) = celld::js::ws_auto_response(scope, ws_id, text) {
            celld::js::ws_emit_batch(vec![(ws_id, celld::js::WsOut::Text(response))]);
            return Ok(());
        }
    }
    let Routed { request, route } = app
        .websocket_request(socket.cell().to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    // The handler's output must reach the gate even when the caller stops
    // waiting. A socket task stops reading, and drops this future, once its own
    // socket closes, but the handler it started runs on regardless. A dropped
    // reply loses a captured batch that already reserved places on other
    // sockets, and a lost batch must close those sockets with 1011 because the
    // gate never answered for it. A task, which a dropped handle only
    // detaches, therefore owns everything from the handoff on. Routing stays
    // above it: a message the cell never received has no output to lose.
    let app = app.clone();
    let scope = scope.to_string();
    let socket = socket.clone();
    let (started, receive) = tokio::sync::oneshot::channel();
    let delivery = celld::asyncrt::spawn(async move {
        deliver_ws_message(&app, &socket, request, ws_id, data, started).await
    });
    // The reader waits only until the first turn. Keep observing completion:
    // ignoring the detached task would hide failures and leave a broken
    // connection open. The delivery still owns its activity and output gate
    // handoff even if this socket closes while the handler is suspended.
    celld::asyncrt::spawn(async move {
        let failure = match delivery.await {
            Ok(Ok(())) => return,
            Ok(Err(error)) => format!("{error:#}"),
            Err(panic) => format!("WebSocket message delivery panicked: {panic}"),
        };
        tracing::warn!(scope, ws_id, %failure, "WebSocket message delivery failed");
        celld::js::ws_close(ws_id, 1011, "WebSocket message delivery failed");
    })
    .detach();
    receive.await.context("WebSocket message did not start")
}

/// Run `webSocketMessage` and hand its output to the cell's barrier queue.
async fn deliver_ws_message(
    app: &AppHandle,
    socket: &SocketRoute,
    request: u64,
    ws_id: u64,
    data: celld::js::WsIn,
    started: tokio::sync::oneshot::Sender<()>,
) -> anyhow::Result<()> {
    let scope = socket.cell();
    let activity = app.activity(request, scope.to_string());
    let runtime = app.runtime.as_ref().context("no cell runtime")?;
    let dispatched = match socket {
        SocketRoute::Cell(cell) => runtime.ws_message(cell.clone(), ws_id, data, started).await,
        SocketRoute::Facet { scope, facet } => {
            runtime
                .facet_ws_message(scope.clone(), facet, ws_id, data, started)
                .await
        }
    };
    let dispatch = match dispatched {
        Ok(dispatch) => dispatch,
        // A handler that failed after it committed opens the barrier a
        // successful writer's batch opens, with no frames behind it: the
        // commit is as unproven either way, and a read-only batch or response
        // that followed would otherwise trail nothing and reveal it (#715).
        // The frames it captured before it failed are dropped, as before.
        //
        // A facet's failed handler opens no barrier. Its position counts in
        // the facet's stream, which the root cell's gate would read as its
        // own, and the facet's next output proves that whole stream anyway.
        Err(error) => {
            let position = match socket {
                SocketRoute::Cell(_) => celld::js::failed_write_position(&error),
                SocketRoute::Facet { .. } => None,
            };
            if let Some(position) = position {
                // The barrier is registered before the activity guard
                // drops: the core reads the still-pinned request when it
                // opens the barrier, and a guard dropped first would let
                // the cell be released under an unregistered write.
                if let Err(stopped) = app
                    .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
                    .await
                {
                    tracing::warn!(scope, position, %stopped, "no barrier for a failed webSocketMessage handler's write");
                }
            }
            drop(activity);
            return Err(error);
        }
    };
    // A read-only frame can reveal another event's unproven write, so all
    // captured frames pass through the cell's barrier queue before emission.
    if !dispatch.frames.is_empty() || dispatch.write_position.is_some() {
        if let Err(stopped) = app
            .ws_output(
                request,
                scope.to_string(),
                dispatch.frames,
                dispatch.write_position,
                dispatch.observed_position,
            )
            .await
        {
            tracing::warn!(scope, %stopped, "no barrier for a webSocketMessage handler's output");
        }
    }
    drop(activity);
    Ok(())
}

async fn dispatch_ws_closed(
    app: &AppHandle,
    socket: &SocketRoute,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    let scope = socket.scope();
    let Routed { request, route } = app
        .websocket_request(socket.cell().to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket close {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let _activity = app.activity(request, socket.cell().to_string());
    let runtime = app.runtime.as_ref().context("no cell runtime")?;
    match socket {
        SocketRoute::Cell(cell) => {
            let answer = runtime
                .ws_closed(cell.clone(), ws_id, code, reason, was_clean)
                .await;
            gate_lifecycle_write(app, request, cell, &answer).await;
            answer.map(|_| ())
        }
        // No lifecycle barrier, for the reason a failed facet message opens
        // none.
        SocketRoute::Facet { scope, facet } => {
            runtime
                .facet_ws_closed(scope.clone(), facet, ws_id, code, reason, was_clean)
                .await
        }
    }
}

/// Open the barrier a lifecycle handler's write needs.
///
/// `webSocketOpen` and `webSocketClose` answer nothing a client sees, so a
/// write they made opens a barrier with no frames behind it, whether the
/// handler returned or failed after it committed. A read-only batch or
/// response that follows then trails that barrier instead of revealing the
/// write while a crash can still lose it (#715).
async fn gate_lifecycle_write(
    app: &AppHandle,
    request: u64,
    scope: &str,
    answer: &anyhow::Result<Option<u64>>,
) {
    let position = match answer {
        Ok(position) => *position,
        Err(error) => celld::js::failed_write_position(error),
    };
    if let Some(position) = position {
        tracing::debug!(
            scope,
            position,
            failed = answer.is_err(),
            "gate lifecycle write"
        );
        if let Err(stopped) = app
            .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
            .await
        {
            tracing::warn!(scope, position, %stopped, "no barrier for a lifecycle handler's write");
        }
    }
}

async fn finish_websocket(
    app: &AppHandle,
    target: &celld::js::WsTarget,
    code: u16,
    reason: String,
    was_clean: bool,
) {
    let socket = SocketRoute::of(target);
    let _ = dispatch_ws_closed(app, &socket, target.id, code, reason, was_clean).await;
    app.websocket_closed(socket.cell().to_string(), target.id);
    celld::js::ws_unregister(target.id);
}

/// Release a WebSocket that the Worker accepted but that celld will not upgrade.
///
/// `accept()` registers the socket and charges it to the scope's regular-socket
/// count before the Worker's response reaches this code, and `WsTarget` is plain
/// data: dropping it releases nothing. A rejection that only returns an error
/// status therefore leaks the registration and the count for the life of the
/// process, and the Worker never sees a close. A remote target is registered on
/// the owner node instead, so the entry node has nothing to release — the same
/// split that the upgrade-failure path makes. 1006 is the code for a connection
/// that closed without a close frame, which is exactly what happened.
pub(super) async fn reject_accepted_websocket(
    app: &AppHandle,
    target: &celld::js::WsTarget,
    reason: &str,
) {
    if target.tunnel.is_none() {
        finish_websocket(app, target, 1006, reason.to_string(), false).await;
    }
}

/// Answer a Worker response on a connection that did not upgrade.
///
/// A response with a socket reaches a client only through `handle_websocket`
/// or a tunneled upgrade, so every other response path answers through this
/// function. workerd checks each response against the request it answers and
/// throws a `TypeError`, which an HTTP client receives as a 500. The object
/// that answers a Durable Object call applies the same check in
/// `dispatch_local_fetch` and `dispatch_forwarded_fetch`. This check therefore
/// covers the top-level Worker's own response, and a path such as `/do/` that
/// cannot upgrade at all.
pub(super) async fn reply_without_upgrade(
    app: &AppHandle,
    mut worker_response: HttpResponse,
    preserve_representation_length: bool,
) -> HttpReply {
    let Some(websocket) = worker_response.websocket.take() else {
        return runtime_response(worker_response, preserve_representation_length);
    };
    refuse_websocket_without_upgrade(app, websocket).await
}

/// Release a socket on a path that cannot upgrade, and answer with a 500.
///
/// Dropping a Worker socket or a tunnel claim releases it, and a local cell
/// socket needs the explicit close. workerd's 500 has an empty body; celld
/// names the `TypeError` in the body, as it does for any other Worker failure.
pub(super) async fn refuse_websocket_without_upgrade(
    app: &AppHandle,
    websocket: HttpResponseWebSocket,
) -> HttpReply {
    let error = format!("TypeError: {WEBSOCKET_WITHOUT_UPGRADE}");
    if let Ok(PendingWebSocket::Cell(target)) = PendingWebSocket::new(websocket) {
        reject_accepted_websocket(app, &target, &error).await;
    }
    response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Worker failed: {error}"),
    )
}

/// One accepted WebSocket response after it returns from JavaScript.
///
/// Keep the response shape in one value through every validation and executor
/// exit. A tunneled response attaches its parked connection lifetime here,
/// so dropping an unfinished response cannot leave host state behind.
enum PendingWebSocket {
    Worker(celld::js::websocket::WorkerWebSocket),
    Upstream(celld::js::websocket::UpstreamWebSocket),
    Cell(celld::js::WsTarget),
    Tunnel {
        target: celld::js::WsTarget,
        upgrade: super::peer_tunnel::TunnelUpgradeClaim,
    },
}

impl PendingWebSocket {
    fn new(websocket: HttpResponseWebSocket) -> anyhow::Result<Self> {
        Ok(match websocket {
            HttpResponseWebSocket::Worker(worker) => Self::Worker(worker),
            HttpResponseWebSocket::Upstream(upstream) => Self::Upstream(upstream),
            HttpResponseWebSocket::Cell(target) => match target.tunnel {
                Some(parked) => Self::Tunnel {
                    target,
                    upgrade: super::peer_tunnel::claim_upgrade(parked)?,
                },
                None => Self::Cell(target),
            },
        })
    }

    fn target(&self) -> Option<&celld::js::WsTarget> {
        match self {
            Self::Worker(_) | Self::Upstream(_) => None,
            Self::Cell(target) => Some(target),
            Self::Tunnel { target, .. } => Some(target),
        }
    }
}

enum OutboundWebSocketSink {
    /// A socket that a Durable Object dialed. A facet's socket routes,
    /// registers, and gates through the facet's root, as an accepted facet
    /// socket does.
    Cell {
        app: Box<AppHandle>,
        socket: SocketRoute,
    },
    Isolate(celld::js::WsPullSender),
}

impl OutboundWebSocketSink {
    async fn open(&self, websocket: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => {
                let cell = socket.cell();
                let Routed { request, route } = app
                    .request(cell.to_string())
                    .await
                    .map_err(|error| anyhow::anyhow!("route outbound WebSocket: {error:?}"))?;
                anyhow::ensure!(
                    route == Route::Local,
                    "outbound WebSocket cell moved off node"
                );
                let _activity = app.activity(request, cell.to_string());
                app.websocket_opened(cell.to_string(), websocket, WebSocketKind::Outbound)
                    .await?;
                let runtime = app.runtime.as_ref().context("no cell runtime")?;
                let answer = match socket {
                    SocketRoute::Cell(cell) => {
                        let answer = runtime.ws_open(cell.clone(), websocket, protocol).await;
                        gate_lifecycle_write(app, request, cell, &answer).await;
                        answer.map(|_| ())
                    }
                    // No lifecycle barrier, for the reason a failed facet
                    // message opens none.
                    SocketRoute::Facet { scope, facet } => {
                        runtime
                            .facet_ws_open(scope.clone(), facet, websocket, protocol)
                            .await
                    }
                };
                if answer.is_err() {
                    app.websocket_closed(cell.to_string(), websocket);
                }
                answer
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Open(protocol))
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn message(&self, websocket: u64, data: celld::js::WsIn) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => dispatch_ws_message(app, socket, websocket, data).await,
            Self::Isolate(tx) => tx
                .send(data.into())
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn closed(
        &self,
        websocket: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => {
                let result =
                    dispatch_ws_closed(app, socket, websocket, code, reason, was_clean).await;
                app.websocket_closed(socket.cell().to_string(), websocket);
                result
            }
            Self::Isolate(tx) => tx
                .send_close(code, reason, was_clean)
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    /// Register the socket before any event of it can be dispatched. A
    /// facet's socket takes its record in the same insertion, so the
    /// registry never holds it under the facet's scope without the route.
    fn register(&self, websocket: u64) {
        if let Self::Cell { socket, .. } = self {
            let facet = match socket {
                SocketRoute::Cell(_) => None,
                SocketRoute::Facet { facet, .. } => Some(facet.clone()),
            };
            celld::js::ws_register_outbound(websocket, socket.scope(), facet);
        }
    }
}

/// One direction of a pipe: the frames an end sent that the end it faces has
/// not yet made room for.
///
/// The pipe keeps reading what an end sends even while the other end's queue is
/// full, so the host registry's unbounded channel never holds more than the
/// pipe has taken out of it. What waits here is capped by a byte budget, and an
/// end that exceeds it is closed. A close the end sent waits here too, behind
/// the data it followed.
struct Lane {
    waiting: std::collections::VecDeque<celld::js::WsPull>,
    bytes: usize,
    end: Option<(u16, String)>,
}

impl Lane {
    fn new() -> Self {
        Self {
            waiting: std::collections::VecDeque::new(),
            bytes: 0,
            end: None,
        }
    }

    /// Hold one frame. False when it overflows the budget. One frame larger
    /// than the budget is held alone, as the queue it waits for admits it.
    fn hold(&mut self, frame: celld::js::WsOut) -> bool {
        let frame = match frame {
            celld::js::WsOut::Text(text) => celld::js::WsPull::Text(text),
            celld::js::WsOut::Binary(bytes) => celld::js::WsPull::Binary(bytes),
            celld::js::WsOut::Close(code, reason) => {
                self.end = Some((code, reason));
                return true;
            }
        };
        let size = frame.queue_bytes();
        if self.bytes > 0 && self.bytes + size > celld::js::WS_PULL_QUEUE_MAX_BYTES {
            return false;
        }
        self.bytes += size;
        self.waiting.push_back(frame);
        true
    }

    /// The size of the frame next in line, if any.
    fn front_bytes(&self) -> Option<usize> {
        self.waiting.front().map(celld::js::WsPull::queue_bytes)
    }

    /// Take the frame next in line out of the backlog.
    fn next(&mut self) -> Option<celld::js::WsPull> {
        let frame = self.waiting.pop_front()?;
        self.bytes -= frame.queue_bytes();
        Some(frame)
    }

    /// Pass the frame next in line to its queue, in the room already reserved.
    /// False when that end stopped reading.
    fn flush(&mut self, sink: &celld::js::WsPullSender, permit: celld::js::WsPullPermit) -> bool {
        match self.next() {
            Some(frame) => sink.send_reserved(frame, permit).is_ok(),
            None => true,
        }
    }

    /// The end has closed and everything it sent before has been passed on.
    fn finished(&self) -> bool {
        self.end.is_some() && self.waiting.is_empty()
    }
}

/// What both ends hear when a pipe's backlog overflows.
fn buffer_full() -> (u16, String) {
    (1009, "WebSocket buffer is full".into())
}

/// Carry frames between a stateless Worker's socket and a client end another
/// isolate in this process kept. The Worker end answered an upgrade as a
/// top-level Worker does, so its frames use the channels it prepared for an
/// external client: its output queue out, its inbound queue in.
///
/// The task ends when either side closes, and it unregisters the client end.
/// Dropping the Worker's socket unregisters its own. Both ends are told how it
/// ended: the close one end sent is echoed to the other once the data it sent
/// before has been passed on, and a pipe that fails closes both with 1011. An
/// end that sends more than a queue's budget while the other end does not read
/// is closed with 1009.
///
/// The close is queued and its entry is left for the isolate to read, which
/// removes it, so an isolate that was not already waiting still reads the code.
async fn worker_websocket_pipe(
    id: u64,
    worker: celld::js::WorkerWebSocket,
    pull: celld::js::WsPullSender,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<celld::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    let inbound = worker.inbound();
    let (caller_tx, mut from_caller) = mpsc::unbounded_channel();
    let (worker_tx, mut from_worker) = mpsc::unbounded_channel();
    // A caller that retired before this task ran has no entry left: its end
    // reads as gone from the first turn and the pipe closes the Worker's.
    celld::js::ws_register_if_open(id, caller_tx);
    celld::js::ws_register(worker.id(), worker_tx);
    let _ = reply.send(Ok(celld::js::OutboundWsOpen {
        protocol: None,
        declined: None,
    }));
    let mut to_worker = Lane::new();
    let mut to_caller = Lane::new();
    let mut closing = (1011, "WebSocket pipe failed".to_string());
    // Nobody reads the caller's queue once its request retired.
    let mut caller_gone = false;
    loop {
        if let Some(end) = to_worker.end.as_ref().filter(|_| to_worker.finished()) {
            closing = end.clone();
            break;
        }
        if let Some(end) = to_caller.end.as_ref().filter(|_| to_caller.finished()) {
            closing = end.clone();
            break;
        }
        let (to_worker_front, to_caller_front) = (to_worker.front_bytes(), to_caller.front_bytes());
        celld::asyncrt::select! {
            frame = from_caller.recv(), if to_worker.end.is_none() => match frame {
                Some(frame) => {
                    if !to_worker.hold(frame) {
                        closing = buffer_full();
                        break;
                    }
                }
                // The caller's request retired and took its socket with it.
                None => {
                    caller_gone = true;
                    to_worker.end = Some((1001, "request ended".into()));
                }
            },
            frame = from_worker.recv(), if to_caller.end.is_none() => match frame {
                Some(frame) => {
                    if !to_caller.hold(frame) {
                        closing = buffer_full();
                        break;
                    }
                }
                None => break,
            },
            permit = async { inbound.reserve(to_worker_front.unwrap_or(0)).await },
                if to_worker_front.is_some() => {
                    let kept = permit.is_some_and(|permit| to_worker.flush(&inbound, permit));
                    if !kept { break; }
                },
            permit = async { pull.reserve(to_caller_front.unwrap_or(0)).await },
                if to_caller_front.is_some() => {
                    let kept = permit.is_some_and(|permit| to_caller.flush(&pull, permit));
                    if !kept { break; }
                },
        }
    }
    // The first close on a queue is the only one, so an end that already
    // closed keeps its own.
    let (code, reason) = closing;
    let _ = inbound.send_close(code, reason.clone(), true);
    let _ = pull.send_close(code, reason, true);
    celld::js::ws_unregister(id);
    if caller_gone {
        celld::js::ws_pull_unregister(id);
    }
    Ok(())
}

/// Carry frames between a Durable Object's socket and a client end another
/// isolate in this process kept, after a `stub.fetch` upgrade.
///
/// A same-isolate pair links its two ends directly and never involves the
/// host. This pair cannot: the cell end lives in the cell's isolate and the
/// client end in the caller's, and neither can reach the other's heap. So
/// each direction takes the route an external client's frames take —
/// `dispatch_ws_message` into the cell, a pull queue out to the caller —
/// which is also why a hibernatable server end needs nothing special here.
///
/// A dispatch waits while the cell is busy or its input gate is closed. The
/// pipe keeps reading both ends meanwhile, into the same per-direction backlog
/// the Worker pipe keeps: a caller that sends more than a queue's budget while
/// the cell does not take frames is closed with 1009, and a close or a
/// vanished end ends the pipe at once. The dispatches go one at a time, in the
/// order the frames came. A close from the caller is answered at once, and the
/// frames it sent before are still given to the cell, then the close, unless
/// that takes longer than the handler budget: then the rest is dropped. What
/// the object says meanwhile is read and dropped.
///
/// The close the caller reads is queued and its entry is left for the isolate
/// to read, which removes it, so an isolate that was not already waiting still
/// reads the code; a caller whose request retired has no reader, so its entry
/// goes at once.
///
/// The task ends when either side closes, and it unregisters both.
async fn local_websocket_pipe(
    app: AppHandle,
    id: u64,
    target: celld::js::WsTarget,
    pull: Option<celld::js::WsPullSender>,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<celld::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    let Some(pull) = pull else {
        let _ = reply.send(Err(anyhow::anyhow!(
            "a bound WebSocket target needs an isolate queue"
        )));
        return Ok(());
    };
    let cell = ObjectSocket {
        app,
        socket: SocketRoute::of(&target),
        id: target.id,
    };
    pipe_to_object(cell, id, pull, reply).await
}

/// What the pipe asks of the Durable Object's end of a socket. The pipe needs
/// no more of the object than these, which is also what lets a test stand in
/// for it.
trait ObjectEnd: Sync {
    /// Give the object a frame, once its cell takes it.
    fn deliver(
        &self,
        data: celld::js::WsIn,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    /// Tell the object's own handler its socket closed.
    fn closed(&self, code: u16, reason: String) -> impl std::future::Future<Output = ()> + Send;
    /// The socket's id on the object's side.
    fn id(&self) -> u64;
    /// The pipe is over: the cell may forget the socket.
    fn release(&self);
}

struct ObjectSocket {
    app: AppHandle,
    socket: SocketRoute,
    id: u64,
}

impl ObjectEnd for ObjectSocket {
    async fn deliver(&self, data: celld::js::WsIn) -> anyhow::Result<()> {
        dispatch_ws_message(&self.app, &self.socket, self.id, data).await
    }

    async fn closed(&self, code: u16, reason: String) {
        let _ = dispatch_ws_closed(&self.app, &self.socket, self.id, code, reason, true).await;
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn release(&self) {
        self.app
            .websocket_closed(self.socket.cell().to_string(), self.id);
    }
}

/// Wait for `work` while reading and dropping what the object sends, so an
/// object that keeps sending during its close handler cannot fill its queue.
async fn discard_while<T>(
    work: impl std::future::Future<Output = T>,
    from_cell: &mut mpsc::UnboundedReceiver<celld::js::WsOut>,
) -> T {
    let mut work = std::pin::pin!(work);
    loop {
        celld::asyncrt::select! {
            result = &mut work => return result,
            frame = from_cell.recv() => if frame.is_none() {
                return work.await;
            },
        }
    }
}

async fn pipe_to_object(
    cell: impl ObjectEnd,
    id: u64,
    pull: celld::js::WsPullSender,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<celld::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    // Both registrations flush whatever each end queued before it had
    // anywhere to send: the cell's greeting frame is queued while its own
    // fetch handler still runs, and the caller can send the moment it
    // accepts.
    let (caller_tx, mut from_caller) = mpsc::unbounded_channel();
    let (cell_tx, mut from_cell) = mpsc::unbounded_channel();
    // A caller that retired before this task ran has no entry left: its end
    // reads as gone from the first turn, as for a Worker pipe.
    celld::js::ws_register_if_open(id, caller_tx);
    celld::js::ws_register(cell.id(), cell_tx);
    let _ = reply.send(Ok(celld::js::OutboundWsOpen {
        protocol: None,
        declined: None,
    }));
    let mut to_cell = Lane::new();
    let mut to_caller = Lane::new();
    // The dispatch of the frame the cell is being given, kept across loop
    // turns so that reading the ends does not cancel it.
    let mut dispatching: Option<
        std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>>,
    > = None;
    let mut dispatched = false;
    // The caller's request retired: nobody reads its queue any more.
    let mut caller_gone = false;
    // Set when the cell's own handler must hear of the end: a close the
    // caller sent, or an overflow that cut it off. A close from the cell needs
    // no entry here: the isolate that sent it has already told the cell's own
    // handler, and telling it again would be a second `webSocketClose`.
    let mut cell_close: Option<(u16, String)> = None;
    // What the caller reads.
    let mut closing = (1011, "WebSocket pipe failed".to_string());
    // Set once the caller closed: nothing more is read, and what it had sent
    // goes to the cell before the close, until the handler budget runs out.
    let mut draining = false;
    let mut drain_ends: Option<celld::asyncrt::Sleep> = None;
    loop {
        if std::mem::take(&mut dispatched) {
            dispatching = None;
        }
        // The cell already closed: what the caller sends now is read and
        // dropped, never given to it.
        let cell_closed = to_caller.end.is_some() && !draining;
        if cell_closed {
            to_cell = Lane::new();
            dispatching = None;
        }
        if dispatching.is_none() {
            let data = match to_cell.next() {
                Some(celld::js::WsPull::Text(text)) => Some(celld::js::WsIn::Text(text)),
                Some(celld::js::WsPull::Binary(bytes)) => Some(celld::js::WsIn::Binary(bytes)),
                _ => None,
            };
            if let Some(data) = data {
                dispatching = Some(Box::pin(cell.deliver(data)));
            }
        }
        if draining {
            if dispatching.is_none() {
                break;
            }
        } else if let Some(end) = to_caller.end.as_ref().filter(|_| to_caller.finished()) {
            closing = end.clone();
            break;
        }
        let to_caller_front = to_caller.front_bytes().filter(|_| !draining);
        celld::asyncrt::select! {
            frame = from_caller.recv(), if !draining => match frame {
                Some(_) if cell_closed => {}
                Some(celld::js::WsOut::Close(code, reason)) => {
                    // The caller is answered and released now; the cell hears
                    // the frames sent before the close, then the close.
                    closing = (code, reason);
                    cell_close = Some(closing.clone());
                    let _ = pull.send_close(closing.0, closing.1.clone(), true);
                    celld::js::ws_unregister(id);
                    draining = true;
                    drain_ends = Some(celld::asyncrt::sleep(celld::js::handler_budget()));
                }
                Some(frame) => {
                    if !to_cell.hold(frame) {
                        closing = buffer_full();
                        cell_close = Some(closing.clone());
                        break;
                    }
                }
                // The caller's request retired and took its socket with it.
                None => {
                    caller_gone = true;
                    break;
                }
            },
            // The caller was already answered once it closed, so what the
            // object says meanwhile is read and dropped: left in the
            // registry's unbounded channel it would grow without a cap.
            frame = from_cell.recv(), if to_caller.end.is_none() || draining => match frame {
                Some(_) if draining => {}
                Some(frame) => {
                    if !to_caller.hold(frame) {
                        closing = buffer_full();
                        cell_close = Some(closing.clone());
                        break;
                    }
                }
                None => break,
            },
            permit = async { pull.reserve(to_caller_front.unwrap_or(0)).await },
                if to_caller_front.is_some() => {
                    let kept = permit.is_some_and(|permit| to_caller.flush(&pull, permit));
                    if !kept { break; }
                },
            () = async { drain_ends.as_mut().expect("guarded").await }, if draining => break,
            result = async { dispatching.as_mut().expect("guarded").await },
                if dispatching.is_some() => {
                    dispatched = true;
                    if let Err(error) = result {
                        tracing::warn!(%error, ws_id = cell.id(), "kept WebSocket message failed");
                        break;
                    }
                },
        }
    }
    drop(dispatching);
    // The close the caller reads: its own, echoed to it; the cell's, once the
    // data before it was passed on; a 1009 when a backlog overflowed; or, for
    // a pipe that ended without a close from either end, 1011 rather than an
    // abnormal end.
    let (code, reason) = closing;
    let _ = pull.send_close(code, reason, true);
    celld::js::ws_unregister(id);
    // The queue stays for the isolate to read the close, which removes it; a
    // request that retired first removes it with its sockets, and one that
    // already retired has nobody to read it.
    if caller_gone {
        celld::js::ws_pull_unregister(id);
    }
    // The cell's handler may wait on a gate that never opens, so hearing of
    // the end is bounded by the handler budget too.
    if let Some((code, reason)) = cell_close {
        // The object keeps its registration, because its close handler may
        // still send, but nobody reads what it sends, so it is dropped as it
        // comes instead of piling up in the unbounded channel.
        let _ = discard_while(
            celld::asyncrt::timeout(celld::js::handler_budget(), cell.closed(code, reason)),
            &mut from_cell,
        )
        .await;
    }
    cell.release();
    celld::js::ws_unregister(cell.id());
    Ok(())
}

pub(crate) async fn outbound_websocket_task(
    app: AppHandle,
    request: celld::js::OutboundWsReq,
) -> anyhow::Result<()> {
    use hyper::header::{HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

    let celld::js::OutboundWsReq {
        scope,
        id,
        url,
        protocols,
        pull,
        headers,
        want_response,
        target,
        worker,
        facet,
        reply,
    } = request;
    if let Some(worker) = worker {
        let Some(pull) = pull else {
            let _ = reply.send(Err(anyhow::anyhow!(
                "a bound WebSocket target needs an isolate queue"
            )));
            return Ok(());
        };
        return worker_websocket_pipe(id, worker, pull, reply).await;
    }
    if let Some(target) = target {
        return local_websocket_pipe(app, id, target, pull, reply).await;
    }
    let sink = match pull {
        Some(pull) => OutboundWebSocketSink::Isolate(pull),
        None => OutboundWebSocketSink::Cell {
            app: Box::new(app),
            socket: match facet {
                Some(facet) => SocketRoute::Facet { scope, facet },
                None => SocketRoute::Cell(scope),
            },
        },
    };
    let mut handshake = HeaderMap::new();
    for (name, value) in &headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        handshake.insert(name, value);
    }
    if !protocols.is_empty() {
        handshake.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    let timeout = std::time::Duration::from_secs(10);
    let connected = tokio::time::timeout(
        timeout,
        celld::ws_client::connect_for_worker(&url, handshake),
    )
    .await;
    let connection = match connected {
        Ok(Ok(connection)) => connection,
        Ok(Err(celld::ws_client::Error::Declined(declined))) if want_response => {
            let declined = celld::js::DeclinedUpgrade {
                status: declined.status.as_u16(),
                headers: declined
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                body: declined.body,
            };
            let _ = reply.send(Ok(celld::js::OutboundWsOpen {
                protocol: None,
                declined: Some(declined),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!("{error}")));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                timeout.as_millis()
            )));
            return Ok(());
        }
    };
    let socket = connection.socket;
    let protocol = connection
        .headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if protocol
        .as_ref()
        .is_some_and(|selected| !protocols.iter().any(|offered| offered == selected))
    {
        let _ = reply.send(Err(anyhow::anyhow!(
            "server selected an unrequested WebSocket subprotocol"
        )));
        return Ok(());
    }

    let (outbound, outputs) = mpsc::unbounded_channel();
    sink.register(id);
    celld::js::ws_register(id, outbound);
    // The room a returned socket's client reserves before it sends. Frames
    // are released as they leave the channel.
    let mut outputs = Accounted {
        room: celld::js::ws_room(id),
        frames: outputs,
    };
    if let Err(error) = sink
        .open(id, protocol.as_deref().unwrap_or_default().to_string())
        .await
    {
        celld::js::ws_unregister(id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply
        .send(Ok(celld::js::OutboundWsOpen {
            protocol,
            declined: None,
        }))
        .is_err()
    {
        let _ = sink
            .closed(id, 1006, "opening event was cancelled".into(), false)
            .await;
        celld::js::ws_unregister(id);
        return Ok(());
    }

    // Ping and Close are answered by the reader's auto-pong and auto-close, as
    // the previous client library also did unprompted. The pump writes those
    // replies on the same socket, so nothing about that changes here.
    let (close, _writer) = {
        let sink = &sink;
        pump_cell_socket(socket, &mut outputs, true, move |data| {
            sink.message(id, data)
        })
        .await
    };
    let _ = sink
        .closed(id, close.state.0, close.state.1, close.state.2)
        .await;
    celld::js::ws_unregister(id);
    Ok(())
}

/// A close frame the peer sent is a clean close, as workerd reports it, even
/// when its payload is malformed and the code becomes a protocol error; only
/// a transport end without a frame (1006) is unclean.
fn websocket_close_details(payload: &[u8]) -> (u16, String) {
    match payload {
        [] => (1005, String::new()),
        [_] => (1002, String::new()),
        [first, second, reason @ ..] => {
            let Ok(reason) = std::str::from_utf8(reason) else {
                return (1007, String::new());
            };
            let code = u16::from_be_bytes([*first, *second]);
            if !celld_logic::schedule::websocket_close_code_is_allowed(code) {
                return (1002, String::new());
            }
            (code, reason.to_string())
        }
    }
}

/// The write half of a cell socket, shared by the reader and the writer.
///
/// The reader needs it because auto-pong and auto-close hand their reply back to
/// the caller once a socket is split. The writer needs it for the cell's own
/// frames, and the close path needs it after both directions stop.
type SocketWriter<S> =
    std::sync::Arc<tokio::sync::Mutex<fastwebsockets::WebSocketWrite<tokio::io::WriteHalf<S>>>>;

/// Close details: the code, the reason, and whether the close was clean.
type CloseState = (u16, String, bool);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseInitiator {
    Peer,
    Application,
    Transport,
}

#[derive(Debug, Eq, PartialEq)]
struct PumpClose {
    state: CloseState,
    initiator: CloseInitiator,
}

async fn write_ws_out<S>(ws: &SocketWriter<S>, out: celld::js::WsOut) -> bool
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::Frame;
    let keep_open = matches!(out, celld::js::WsOut::Text(_) | celld::js::WsOut::Binary(_));
    let frame = match out {
        celld::js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        celld::js::WsOut::Binary(data) => Frame::binary(data.into()),
        celld::js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
    };
    ws.lock().await.write_frame(frame).await.is_ok() && keep_open
}

async fn echo_websocket_close<S>(
    writer: &SocketWriter<S>,
    close: &CloseState,
    application_sent_close: bool,
) where
    S: tokio::io::AsyncWrite + Unpin,
{
    if let Some(code) =
        celld_logic::schedule::websocket_echo_close(close.0, close.2, application_sent_close)
    {
        let _ = write_ws_out(writer, celld::js::WsOut::Close(code, close.1.clone())).await;
    }
}

/// How long the connector of a returned socket waits for one write to its
/// upstream. A client that waits for room stops reading, so nothing else
/// notices that it left while the upstream reads nothing; the write ending is
/// what frees the connection then.
const UPSTREAM_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Where the frames a pump writes come from. `recv` must be cancel-safe.
trait Outputs {
    fn recv(&mut self) -> impl std::future::Future<Output = Option<celld::js::WsOut>> + Send;

    /// How long one write may take before the socket is given up.
    fn write_timeout(&self) -> Option<std::time::Duration> {
        None
    }
}

impl Outputs for mpsc::UnboundedReceiver<celld::js::WsOut> {
    fn recv(&mut self) -> impl std::future::Future<Output = Option<celld::js::WsOut>> + Send {
        mpsc::UnboundedReceiver::recv(self)
    }
}

/// The connector's channel, giving back the room a client reserved for each
/// frame as the frame leaves the channel.
struct Accounted {
    frames: mpsc::UnboundedReceiver<celld::js::WsOut>,
    room: std::sync::Arc<celld::js::WsRoom>,
}

impl Outputs for Accounted {
    async fn recv(&mut self) -> Option<celld::js::WsOut> {
        let out = self.frames.recv().await?;
        self.room.release(celld::js::ws_out_bytes(&out));
        Some(out)
    }

    fn write_timeout(&self) -> Option<std::time::Duration> {
        self.room.is_counting().then_some(UPSTREAM_WRITE_TIMEOUT)
    }
}

impl Drop for Accounted {
    fn drop(&mut self) {
        self.room.close();
    }
}

/// An upstream's own pull queue, read in place: the frames it sent wait in
/// the queue, which holds at most `WS_PULL_QUEUE_MAX_BYTES` and stops
/// reading the upstream beyond that.
impl Outputs for celld::js::WsPullReceiver {
    async fn recv(&mut self) -> Option<celld::js::WsOut> {
        loop {
            return Some(match celld::js::WsPullReceiver::recv(self).await? {
                celld::js::WsPull::Text(text) => celld::js::WsOut::Text(text),
                celld::js::WsPull::Binary(bytes) => celld::js::WsOut::Binary(bytes),
                celld::js::WsPull::Open(_) => continue,
                // A code that cannot go on the wire (1005, 1006) is the
                // upstream ending without a close of its own: the client is
                // told as a failed pipe is.
                celld::js::WsPull::Close(code, reason, clean) => {
                    match celld_logic::schedule::websocket_echo_close(code, clean, false) {
                        Some(code) => celld::js::WsOut::Close(code, reason),
                        None => celld::js::WsOut::Close(1011, "upstream connection lost".into()),
                    }
                }
            });
        }
    }
}

/// Carry frames between a socket and the cell behind it until one side stops.
///
/// The read is never cancelled. `fastwebsockets::read_frame` consumes header
/// bytes from its buffer and then awaits for the payload, holding what it parsed
/// in local variables, so dropping that future keeps the buffer advanced and
/// loses the header. The next read then treats a payload byte as a frame header
/// and the stream never realigns. Earlier versions of both callers read the
/// socket in the same select as the cell's outbound queue, which drops the losing
/// future on every iteration. Cancelling a read mid-payload fails against that
/// shape.
///
/// One async block therefore owns each direction. The halves are split so that
/// the writer can write while the reader reads, and the writer stops on a signal
/// instead of being dropped, because dropping it can leave a partial frame on
/// the socket.
///
/// The caller sets `auto_close` and `auto_pong` on the socket before the pump
/// runs, and the pump keeps that choice: an automatic reply is written on the
/// same socket, exactly as the unsplit collector wrote it.
///
/// `outbound_close_is_clean` reports a close that the application sends as the
/// close state of the socket. The local path leaves that false, because it
/// answers an unclean end with its own protocol echo after the pump returns.
/// The initiator remains separate because only a peer close needs that echo.
///
/// The returned writer is still live, so the caller can write the close frames
/// that follow.
async fn pump_cell_socket<S, F, Fut>(
    socket: fastwebsockets::WebSocket<S>,
    outputs: &mut impl Outputs,
    outbound_close_is_clean: bool,
    mut inbound: F,
) -> (PumpClose, SocketWriter<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(celld::js::WsIn) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    use fastwebsockets::{FragmentCollectorRead, Frame as WsFrame, OpCode, Payload};

    let (reader, writer) = socket.split(tokio::io::split);
    let mut reader = FragmentCollectorRead::new(reader);
    let writer: SocketWriter<S> = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    let mut close = PumpClose {
        state: (1006, String::new(), false),
        initiator: CloseInitiator::Transport,
    };
    let (stop_writer, mut stopped) = tokio::sync::oneshot::channel::<()>();
    {
        let obligated_writer = writer.clone();
        let mut obligated = move |frame: WsFrame<'_>| {
            let writer = obligated_writer.clone();
            // The reply borrows the reader's buffer, and the write outlives the
            // callback, so the payload is copied out. The reader builds a pong
            // or a close echo unmasked, and `write_frame` masks it if the role
            // needs it, exactly as the unsplit collector did.
            let reply = WsFrame::new(
                frame.fin,
                frame.opcode,
                None,
                Payload::Owned(frame.payload.to_vec()),
            );
            async move { writer.lock().await.write_frame(reply).await }
        };

        let read = async {
            loop {
                let Ok(frame) = reader.read_frame(&mut obligated).await else {
                    return None;
                };
                let delivered = match frame.opcode {
                    OpCode::Text => {
                        inbound(celld::js::WsIn::Text(
                            String::from_utf8_lossy(&frame.payload).into_owned(),
                        ))
                        .await
                    }
                    OpCode::Binary => {
                        inbound(celld::js::WsIn::Binary(frame.payload.to_vec())).await
                    }
                    OpCode::Close => {
                        let (code, reason) = websocket_close_details(&frame.payload);
                        return Some(PumpClose {
                            state: (code, reason, true),
                            initiator: CloseInitiator::Peer,
                        });
                    }
                    _ => Ok(()),
                };
                if delivered.is_err() {
                    return None;
                }
            }
        };

        // `recv` and the stop signal are both cancel-safe, and the write runs in
        // the branch body, so no write is ever cancelled either.
        let write = async {
            loop {
                celld::asyncrt::select_biased! {
                    "a stop signal that ties an outbound frame prevents one more socket write";
                    _ = &mut stopped => return None,
                    output = Outputs::recv(&mut *outputs) => {
                        let output = output?;
                        let closed = match &output {
                            celld::js::WsOut::Close(code, reason) => Some(PumpClose {
                                state: if outbound_close_is_clean {
                                    (*code, reason.clone(), true)
                                } else {
                                    (1006, String::new(), false)
                                },
                                initiator: CloseInitiator::Application,
                            }),
                            _ => None,
                        };
                        let written = match outputs.write_timeout() {
                            Some(limit) => tokio::time::timeout(limit, write_ws_out(&writer, output))
                                .await
                                .unwrap_or(false),
                            None => write_ws_out(&writer, output).await,
                        };
                        if !written { return closed; }
                    }
                }
            }
        };

        let mut read = std::pin::pin!(read);
        let mut write = std::pin::pin!(write);
        celld::asyncrt::select! {
            result = &mut read => {
                if let Some(details) = result { close = details; }
                // Stop the writer between frames rather than dropping it, so a
                // frame it started reaches the socket whole.
                let _ = stop_writer.send(());
                let _ = write.await;
            }
            result = &mut write => {
                if let Some(details) = result { close = details; }
                // The read is dropped here. That is safe only because the pump
                // is over and the socket is never read again.
            }
        }
    }
    (close, writer)
}

/// Carry a client socket into the top-level Worker request that returned it.
///
/// A Durable Object has a stable scope, so its frames can return through a
/// later cell dispatch. A stateless Worker has no such address. Its pending
/// `__ws_next` operation therefore keeps this request and isolate resident,
/// and this task carries each client frame through that operation's queue.
async fn worker_websocket_task<S>(
    worker: celld::js::WorkerWebSocket,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let id = worker.id();
    let inbound = worker.inbound();
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(id, outbound);
    let (close, writer) = pump_cell_socket(socket, &mut outputs, true, move |data| {
        let inbound = inbound.clone();
        async move {
            inbound
                .send(data.into())
                .await
                .map_err(|_| anyhow::anyhow!("Worker stopped reading WebSocket"))
        }
    })
    .await;
    if close.initiator == CloseInitiator::Peer {
        echo_websocket_close(&writer, &close.state, false).await;
    }
    let _ = worker
        .inbound()
        .send_close(close.state.0, close.state.1, close.state.2);
}

/// Carry a client socket to the upstream connection a Worker opened with
/// `fetch()` and returned. The Worker's isolate is out of the path: the
/// upstream's queue is read in place for what goes to the client, and the
/// client's frames go straight to the connection's writer, one hop each way.
/// A close either side sends reaches the other with its code and reason.
async fn upstream_websocket_task<S>(
    mut upstream: celld::js::websocket::UpstreamWebSocket,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let Some(mut frames) = upstream.take_pull() else {
        return;
    };
    let link = upstream.link();
    let (close, writer) = pump_cell_socket(socket, &mut frames, true, {
        let link = link.clone();
        move |data| {
            let frame = match data {
                celld::js::WsIn::Text(text) => celld::js::WsOut::Text(text),
                celld::js::WsIn::Binary(bytes) => celld::js::WsOut::Binary(bytes),
            };
            let link = link.clone();
            // Waiting for room stops the read of the client, whose own
            // connection then pushes back, while the upstream does not take
            // frames. Once the upstream is gone the frame is dropped and the
            // client is still read, so the frames and close it left in the
            // queue reach the client.
            async move {
                link.send(frame).await;
                Ok(())
            }
        }
    })
    .await;
    match close.initiator {
        CloseInitiator::Peer => {
            echo_websocket_close(&writer, &close.state, false).await;
            let code =
                celld_logic::schedule::websocket_echo_close(close.state.0, close.state.2, false)
                    .unwrap_or(1011);
            link.close(code, &close.state.1);
        }
        CloseInitiator::Transport => link.close(1001, "client went away"),
        // The upstream closed first and its close was written to the client.
        CloseInitiator::Application => return,
    }
    // The connector writes the close once it is free of what the upstream
    // still sends, which only a reader of the queue allows.
    celld::js::ws_pull_drain(&mut frames).await;
}

pub(super) async fn websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let route = SocketRoute::of(&target);
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(target.id, outbound);
    // A close the cell sends does not become the close state here: the echo
    // below decides what the peer observes, so the pump must not claim the
    // close was clean.
    let (close, writer) = {
        let app = &app;
        let route = &route;
        let id = target.id;
        pump_cell_socket(socket, &mut outputs, false, move |data| {
            dispatch_ws_message(app, route, id, data)
        })
        .await
    };
    if let Err(error) = dispatch_ws_closed(
        &app,
        &route,
        target.id,
        close.state.0,
        close.state.1.clone(),
        close.state.2,
    )
    .await
    {
        tracing::warn!(
            %error,
            scope = %target.scope,
            websocket = target.id,
            "WebSocket close dispatch failed"
        );
    }

    // The close handler is allowed to choose the response code and reason.
    // Its output is queued while dispatch_ws_closed drives V8, so flush it
    // before unregistering the socket or considering a protocol-level echo.
    //
    // Its output can also still be behind the output gate: the handler may
    // read, and what it reads can belong to a write another request has not
    // proved durable yet. The drain below does not block, so a frame still in
    // flight leaves `handler_sent_close` false and the peer is answered with
    // the echo of its own close -- a clean close carrying the wrong reason,
    // which is indistinguishable from the handler choosing it.
    //
    // Bounded, and skipped outright on a draining node. The drain loop polls
    // the gate calls it already dispatched but never receives a new one: it
    // has no `gate_rx` arm. A ticket taken after the main loop broke is sent
    // to a live but unread channel and parks for good, so waiting on it here
    // would hold the socket task open until the drain hits its deadline. The
    // bound covers the same park reached the other way: `is_draining` is read
    // once, and shutdown can begin between that read and the wait. Giving up
    // costs the reason the handler chose, never a frame the gate has not
    // cleared -- the drain below still finds nothing, the echo still answers
    // the peer, and a frame released later lands on a socket that is gone.
    if !app.is_draining() {
        let waited = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            celld::js::ws_await_flushes(target.id),
        )
        .await;
        if waited.is_err() {
            tracing::warn!(
                scope = %target.scope,
                websocket = target.id,
                "gave up waiting for the close handler's frames"
            );
        }
    }
    let mut handler_sent_close = false;
    while let Ok(output) = outputs.try_recv() {
        handler_sent_close |= matches!(output, celld::js::WsOut::Close(_, _));
        if !write_ws_out(&writer, output).await {
            break;
        }
    }
    echo_websocket_close(&writer, &close.state, handler_sent_close).await;
    app.websocket_closed(route.cell().to_string(), target.id);
    celld::js::ws_unregister(target.id);
}

/// Headers that the WebSocket server must compute or remove for a 101.
///
/// `fastwebsockets` creates its response before the Worker runs, so blindly
/// appending the Worker's response afterwards can replace the accept hash or
/// restore framing fields that an upgrade cannot carry. This is the exclusion
/// set in kj's `acceptWebSocket`: it forces the three handshake fields, drops
/// the obsolete or request-only fields, and drops `Sec-WebSocket-Extensions`.
fn forwards_worker_websocket_header(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "upgrade"
            | "sec-websocket-accept"
            | "keep-alive"
            | "te"
            | "trailer"
            | "content-length"
            | "transfer-encoding"
            | "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
    )
}

pub(crate) async fn handle_websocket(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let started = Instant::now();
    let cancellation = celld::runtime::RequestCancellationLifetime::stateless();
    let request_id = cancellation.request_id();
    let (mut upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => return response(StatusCode::BAD_REQUEST, format!("ws upgrade: {error}")),
    };
    let runtime = app.runtime.as_ref().expect("WebSocket runtime checked");
    let body_started = Instant::now();
    let (url, method, body, headers) = match request_payload(
        request,
        app.trust_forwarded_headers,
        app.max_request_body_bytes,
    )
    .await
    {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    let body_read_us = body_started.elapsed().as_micros() as u64;
    // The subprotocols the client offered: a 101 may name only one of them.
    let offered: Vec<String> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-protocol"))
        .flat_map(|(_, value)| value.split(','))
        .map(|protocol| protocol.trim().to_string())
        .collect();
    let worker_started = Instant::now();
    let mut worker_response = match runtime
        .fetch_worker_pool(url, method, body.into(), headers, cancellation)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebSocketConnectionOutcome {
                    outcome: "worker_error",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            );
        }
    };
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    let websocket = match worker_response.websocket.take() {
        Some(websocket) => match PendingWebSocket::new(websocket) {
            Ok(websocket) => websocket,
            Err(error) => {
                return response(
                    StatusCode::BAD_GATEWAY,
                    format!("tunneled WebSocket upgrade: {error}"),
                );
            }
        },
        None => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: "",
                    status: worker_response.status,
                },
            );
            return runtime_response(worker_response, false);
        }
    };
    let target = websocket.target();
    if worker_response.status != 101 {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: target.map_or("", |target| target.scope.as_str()),
                status: worker_response.status,
            },
        );
        if let Some(target) = target {
            reject_accepted_websocket(&app, target, "unsupported WebSocket route").await;
        }
        return response(StatusCode::BAD_GATEWAY, "unsupported WebSocket route");
    }
    // Workerd shallow-copies the Worker's complete header list into
    // `acceptWebSocket`, which preserves duplicate application fields such as
    // `Set-Cookie`. Validate the complete batch before changing the response,
    // because returning a partially copied handshake is never useful.
    let mut worker_headers = Vec::new();
    for (name, value) in &worker_response.headers {
        if !forwards_worker_websocket_header(name) {
            continue;
        }
        // The subprotocol a returned fetch() socket names is the upstream's
        // choice; the client gets it only if it offered it.
        if matches!(websocket, PendingWebSocket::Upstream(_))
            && name.eq_ignore_ascii_case("sec-websocket-protocol")
            && !offered.contains(value)
        {
            continue;
        }
        let parsed = hyper::header::HeaderName::from_bytes(name.as_bytes())
            .ok()
            .zip(hyper::header::HeaderValue::from_str(value).ok());
        let Some(header) = parsed else {
            let reason = format!("invalid WebSocket response header: {name}");
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: target.map_or("", |target| target.scope.as_str()),
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                },
            );
            if let Some(target) = target {
                reject_accepted_websocket(&app, target, &reason).await;
            }
            return response(StatusCode::BAD_GATEWAY, "invalid WebSocket response header");
        };
        worker_headers.push(header);
    }
    for (name, value) in worker_headers {
        upgrade_response.headers_mut().append(name, value);
    }
    emit_websocket_connection_timing(
        runtime,
        request_id,
        started,
        body_read_us,
        worker_dispatch_us,
        WebSocketConnectionOutcome {
            outcome: "accepted",
            route: match target {
                Some(target) if target.tunnel.is_some() => "remote",
                Some(_) => "local",
                None => "worker",
            },
            scope: target.map_or("", |target| target.scope.as_str()),
            status: 101,
        },
    );
    let task_app = app.clone();
    let task = Box::pin(run_websocket_upgrade(task_app, upgrade, websocket));
    if app.websockets.send(task).is_err() {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        );
    }
    upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync())
}

/// Finish the client upgrade through the same owned response value that
/// survives validation. Keeping this work in one future makes a failed client
/// upgrade and a cancelled executor task drop the same owned response.
async fn run_websocket_upgrade(
    task_app: AppHandle,
    upgrade: fastwebsockets::upgrade::UpgradeFut,
    websocket: PendingWebSocket,
) {
    match (upgrade.await, websocket) {
        (Ok(socket), PendingWebSocket::Worker(worker)) => {
            worker_websocket_task(worker, socket).await;
        }
        (Ok(socket), PendingWebSocket::Upstream(upstream)) => {
            upstream_websocket_task(upstream, socket).await;
        }
        (Ok(socket), PendingWebSocket::Tunnel { upgrade, .. }) => {
            if let Err(error) = super::peer_tunnel::splice(socket.into_inner(), upgrade).await {
                eprintln!("celld tunneled WebSocket splice ended: {error:#}");
            }
        }
        (Ok(socket), PendingWebSocket::Cell(target)) => {
            websocket_task(task_app, target, socket).await;
        }
        (Err(error), PendingWebSocket::Worker(_) | PendingWebSocket::Upstream(_)) => {
            eprintln!("celld Worker WebSocket upgrade failed: {error}");
        }
        (Err(error), PendingWebSocket::Cell(target)) => {
            eprintln!("celld WebSocket upgrade failed: {error}");
            finish_websocket(&task_app, &target, 1006, String::new(), false).await;
        }
        (Err(error), PendingWebSocket::Tunnel { .. }) => {
            eprintln!("celld tunneled WebSocket upgrade failed: {error}");
        }
    }
}

struct WebSocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    runtime: &RuntimeManager,
    request_id: celld::js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebSocketConnectionOutcome<'_>,
) {
    let WebSocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    tracing::debug!(
        target: "timing",
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %celld::js::request_id_string(request_id),
        node = runtime.node(),
        region = runtime.region(),
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

// Both cell socket loops had the same hazard as the tunnel: they read the socket
// in the same select! as the cell's outbound queue, so an outbound frame that
// won the race dropped a read in progress. A lost frame keeps the workload
// ledger exact while delivery is not, so frame-level observation is required.

#[cfg(test)]
mod pipe_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// An object whose cell takes a frame only when the test lets it.
    struct Object {
        id: u64,
        gate: Arc<tokio::sync::Semaphore>,
        log: Arc<Mutex<Vec<String>>>,
        close_gate: Arc<tokio::sync::Semaphore>,
    }

    impl ObjectEnd for Object {
        async fn deliver(&self, data: celld::js::WsIn) -> anyhow::Result<()> {
            self.gate.acquire().await?.forget();
            if let celld::js::WsIn::Text(text) = data {
                self.log.lock().unwrap().push(text);
            }
            Ok(())
        }

        async fn closed(&self, code: u16, _reason: String) {
            self.close_gate.acquire().await.unwrap().forget();
            self.log.lock().unwrap().push(format!("close:{code}"));
        }

        fn id(&self) -> u64 {
            self.id
        }

        fn release(&self) {}
    }

    struct Pipe {
        caller: u64,
        object: u64,
        gate: Arc<tokio::sync::Semaphore>,
        log: Arc<Mutex<Vec<String>>>,
        close_gate: Arc<tokio::sync::Semaphore>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    /// The drain's timer runs on the process runtime, bound once per process.
    /// A test binds it before it registers anything, so the registries it
    /// uses stay the same ones for its whole run.
    fn host() {
        static HOST: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        HOST.get_or_init(|| {
            std::env::set_var("CELLD_HANDLER_BUDGET_S", "1");
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("host runtime");
            celld::asyncrt::set_host_handle(runtime.handle().clone());
            runtime
        });
    }

    /// Starts a pipe between caller socket `caller` and object socket
    /// `caller + 1`, with the object's cell not taking frames yet.
    async fn start(caller: u64) -> Pipe {
        start_with(caller, false).await
    }

    /// Like `start`, but with the caller's request retired before the pipe
    /// task first runs when `retired` is set.
    async fn start_with(caller: u64, retired: bool) -> Pipe {
        host();
        let object = caller + 1;
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let close_gate = Arc::new(tokio::sync::Semaphore::new(1000));
        let (pull, receiver) = celld::js::ws_pull_channel();
        celld::js::ws_pull_register(caller, receiver);
        celld::js::ws_register_outbound(caller, "", None);
        if retired {
            // What the request's end does to its socket.
            celld::js::ws_pull_unregister(caller);
            celld::js::ws_unregister(caller);
        }
        let (reply, opened) = tokio::sync::oneshot::channel();
        let cell = Object {
            id: object,
            gate: gate.clone(),
            log: log.clone(),
            close_gate: close_gate.clone(),
        };
        let task = tokio::spawn(pipe_to_object(cell, caller, pull, reply));
        opened.await.unwrap().unwrap();
        Pipe {
            caller,
            object,
            gate,
            log,
            close_gate,
            task,
        }
    }

    fn emit(id: u64, frame: celld::js::WsOut) {
        celld::js::ws_emit_batch(vec![(id, frame)]);
    }

    async fn ended(task: tokio::task::JoinHandle<anyhow::Result<()>>) -> bool {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn an_object_that_keeps_sending_during_its_close_handler_is_read() {
        let (tx, mut from_cell) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let work = {
            let release = release.clone();
            async move { release.notified().await }
        };
        let task = tokio::spawn(async move {
            discard_while(work, &mut from_cell).await;
            from_cell
        });
        for _ in 0..64 {
            tx.send(celld::js::WsOut::Text("x".into())).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The handler is still running, and the frames are already gone.
        assert!(!task.is_finished());
        release.notify_one();
        let from_cell = task.await.unwrap();
        assert!(from_cell.is_empty(), "the object's frames piled up");
    }

    #[tokio::test]
    async fn a_draining_pipe_keeps_reading_what_the_object_says() {
        let pipe = start(9_200_000).await;
        emit(pipe.caller, celld::js::WsOut::Text("kept".into()));
        emit(pipe.caller, celld::js::WsOut::Close(3001, "done".into()));
        // The object's cell is shut, so the pipe is draining. Frames the
        // object sends now are read and dropped, and the pipe still waits.
        for _ in 0..8 {
            emit(pipe.object, celld::js::WsOut::Text("x".into()));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!pipe.task.is_finished(), "the pipe ended before the drain");
        // Reading shows when the object's end went away, which a pipe that
        // never reads it cannot see until the handler budget runs out.
        celld::js::ws_unregister(pipe.object);
        assert!(
            ended(pipe.task).await,
            "the object's end was not being read"
        );
        // The frame the cell never took is dropped; its handler hears the close.
        assert_eq!(*pipe.log.lock().unwrap(), ["close:3001"]);
        celld::js::ws_pull_unregister(pipe.caller);
    }

    #[tokio::test]
    async fn a_draining_pipe_still_gives_the_object_what_the_caller_sent() {
        let pipe = start(9_200_010).await;
        emit(pipe.caller, celld::js::WsOut::Text("one".into()));
        emit(pipe.caller, celld::js::WsOut::Text("two".into()));
        emit(pipe.caller, celld::js::WsOut::Close(3002, "done".into()));
        emit(pipe.object, celld::js::WsOut::Text("ignored".into()));
        pipe.gate.add_permits(2);
        assert!(ended(pipe.task).await);
        assert_eq!(*pipe.log.lock().unwrap(), ["one", "two", "close:3002"]);
        celld::js::ws_pull_unregister(pipe.caller);
    }

    #[tokio::test]
    async fn a_caller_that_is_not_reading_still_gets_the_close_it_was_sent() {
        for (caller, close_from_caller) in [(9_200_020, true), (9_200_030, false)] {
            let pipe = start(caller).await;
            let (sender, code) = if close_from_caller {
                (pipe.caller, 3003)
            } else {
                (pipe.object, 3004)
            };
            emit(sender, celld::js::WsOut::Close(code, "done".into()));
            if close_from_caller {
                pipe.gate.add_permits(1);
            }
            assert!(ended(pipe.task).await);
            // Nobody has read the caller's queue yet: it must still be there.
            assert!(
                celld::js::ws_pull_registered(caller),
                "the caller's queue went with the pipe (close {code})"
            );
            celld::js::ws_pull_unregister(caller);
        }
    }

    #[tokio::test]
    async fn a_caller_whose_request_retired_leaves_no_queue() {
        let pipe = start(9_200_040).await;
        // The request's socket goes with it, and the caller's sender drops.
        celld::js::ws_unregister(pipe.caller);
        assert!(ended(pipe.task).await);
        assert!(!celld::js::ws_pull_registered(pipe.caller));
    }

    #[tokio::test]
    async fn a_caller_retired_before_the_pipe_starts_registers_nothing() {
        let pipe = start_with(9_200_045, true).await;
        // The object's end is told the caller left, and the pipe ends.
        pipe.close_gate.add_permits(1);
        assert!(ended(pipe.task).await, "the pipe outlived its caller");
        assert!(!celld::js::ws_output_registered(pipe.caller));
        assert!(!celld::js::ws_output_registered(pipe.object));
        assert!(!celld::js::ws_pull_registered(pipe.caller));
    }

    #[tokio::test]
    async fn a_worker_pipe_whose_caller_retired_before_it_starts_closes_the_worker() {
        host();
        let (caller, worker_id) = (9_200_047, 9_200_048);
        let (inbound, _worker_queue) = celld::js::ws_pull_channel();
        let (pull, _caller_queue) = celld::js::ws_pull_channel();
        // The caller's request ended before the pipe task ran: its entry is gone.
        celld::js::ws_register_outbound(caller, "", None);
        celld::js::ws_unregister(caller);
        let (reply, _opened) = tokio::sync::oneshot::channel();
        let worker = celld::js::WorkerWebSocket::new(worker_id, inbound.clone());
        let task = tokio::spawn(worker_websocket_pipe(caller, worker, pull, reply));
        assert!(ended(task).await, "the pipe outlived its caller");
        assert!(inbound.is_closed(), "the Worker's end was left open");
        assert!(!celld::js::ws_output_registered(caller));
        assert!(!celld::js::ws_output_registered(worker_id));
    }

    #[tokio::test]
    async fn a_close_the_object_never_takes_does_not_hold_the_pipe() {
        let pipe = start(9_200_050).await;
        // The object's input gate stays shut: its close handler never runs.
        pipe.close_gate.forget_permits(1000);
        emit(pipe.caller, celld::js::WsOut::Close(3005, "done".into()));
        assert!(
            ended(pipe.task).await,
            "the pipe waited on a close the object never took"
        );
        assert!(pipe.log.lock().unwrap().is_empty());
        celld::js::ws_pull_unregister(pipe.caller);
    }

    #[tokio::test]
    async fn a_closed_object_is_not_given_what_the_caller_sends_after() {
        let pipe = start(9_200_060).await;
        // The first frame takes the caller's queue; the second waits behind it
        // in the pipe, so the object's close waits too.
        let big = "x".repeat(celld::js::WS_PULL_QUEUE_MAX_BYTES);
        emit(pipe.object, celld::js::WsOut::Text(big.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        emit(pipe.object, celld::js::WsOut::Text(big));
        emit(pipe.object, celld::js::WsOut::Close(3006, "done".into()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        pipe.gate.add_permits(10);
        emit(pipe.caller, celld::js::WsOut::Text("late".into()));
        emit(pipe.caller, celld::js::WsOut::Close(3007, "late".into()));
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The caller leaves without reading: the pipe ends.
        celld::js::ws_pull_unregister(pipe.caller);
        assert!(ended(pipe.task).await);
        // The object closed itself: it gets neither frame nor a second close.
        assert!(pipe.log.lock().unwrap().is_empty(), "{:?}", pipe.log);
    }

    /// What an upstream's queue gives a client to write: data as it came, a
    /// clean close with its code and reason, and a code that cannot go on the
    /// wire as a 1011.
    #[tokio::test]
    async fn an_upstream_queue_is_written_to_the_client_as_frames_and_a_wire_close() {
        for (close, expected) in [
            (
                (4002, "bye", true),
                celld::js::WsOut::Close(4002, "bye".into()),
            ),
            ((1005, "", true), celld::js::WsOut::Close(1000, "".into())),
            (
                (1006, "", false),
                celld::js::WsOut::Close(1011, "upstream connection lost".into()),
            ),
        ] {
            let (tx, mut rx) = celld::js::ws_pull_channel();
            assert!(tx
                .send(celld::js::WsPull::Open("proto".into()))
                .await
                .is_ok());
            assert!(tx.send(celld::js::WsPull::Text("a".into())).await.is_ok());
            assert!(tx.send(celld::js::WsPull::Binary(vec![1])).await.is_ok());
            assert!(tx.send_close(close.0, close.1.into(), close.2).is_ok());
            let mut out = Vec::new();
            while let Some(frame) = Outputs::recv(&mut rx).await {
                out.push(frame);
            }
            let got = format!("{out:?}");
            let want = format!(
                "{:?}",
                vec![
                    celld::js::WsOut::Text("a".into()),
                    celld::js::WsOut::Binary(vec![1]),
                    expected
                ]
            );
            assert_eq!(got, want);
        }
    }

    /// A client that sends faster than the upstream reads waits for room
    /// instead of growing the queue, and goes on as the connector drains it.
    #[tokio::test]
    async fn a_client_is_held_while_the_upstream_queue_is_full() {
        let room = std::sync::Arc::new(celld::js::WsRoom::default());
        let frame = celld::js::WsOut::Binary(vec![0; 64 * 1024]);
        let size = celld::js::ws_out_bytes(&frame);
        let mut held = 0;
        while tokio::time::timeout(Duration::from_millis(100), room.reserve(size))
            .await
            .is_ok()
        {
            held += 1;
            assert!(held < 1000, "the room never filled");
        }
        assert!(room.queued() <= celld::js::WS_PULL_QUEUE_MAX_BYTES);
        assert!(held * size + size > celld::js::WS_PULL_QUEUE_MAX_BYTES);
        let waiting = tokio::spawn({
            let room = room.clone();
            async move { room.reserve(size).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());
        room.release(size);
        assert!(waiting.await.unwrap());
        assert!(room.queued() <= celld::js::WS_PULL_QUEUE_MAX_BYTES);
        let gone = tokio::spawn({
            let room = room.clone();
            async move { room.reserve(size).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        room.close();
        assert!(!gone.await.unwrap(), "a closed room refuses");
    }

    /// A room counts nothing until a client first sends, so a connector that
    /// nobody sends to pays no more than a load per frame.
    #[tokio::test]
    async fn a_room_counts_nothing_before_the_first_reservation() {
        let room = celld::js::WsRoom::default();
        room.release(1000);
        assert!(!room.is_counting());
        assert!(room.reserve(10).await);
        assert!(room.is_counting());
        assert_eq!(room.queued(), 10);
        room.release(1000);
        assert_eq!(room.queued(), 0, "a release never goes below zero");
    }

    /// Frames for a connection whose upstream takes nothing.
    struct Stalling(mpsc::UnboundedReceiver<celld::js::WsOut>);

    impl Outputs for Stalling {
        fn recv(&mut self) -> impl std::future::Future<Output = Option<celld::js::WsOut>> + Send {
            mpsc::UnboundedReceiver::recv(&mut self.0)
        }

        fn write_timeout(&self) -> Option<Duration> {
            Some(Duration::from_millis(200))
        }
    }

    /// A connector whose upstream never reads gives up the write that is stuck
    /// instead of holding the connection, and the client that waited for room
    /// behind it, for good.
    #[tokio::test]
    async fn a_pump_gives_up_a_write_the_peer_never_takes() {
        let (near, _far) = tokio::io::duplex(1024);
        let socket = fastwebsockets::WebSocket::after_handshake(near, fastwebsockets::Role::Client);
        let (frames, receiver) = mpsc::unbounded_channel();
        frames
            .send(celld::js::WsOut::Binary(vec![0; 256 * 1024]))
            .unwrap();
        let mut outputs = Stalling(receiver);
        let ended = tokio::time::timeout(
            Duration::from_secs(10),
            pump_cell_socket(socket, &mut outputs, true, |_| async { Ok(()) }),
        )
        .await;
        let (close, _writer) = ended.expect("the pump held a write the peer never took");
        assert_eq!(close.state.0, 1006);
        drop(frames);
    }
}

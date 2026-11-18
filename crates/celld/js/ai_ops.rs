//! The V8 surface over the `ai` binding: the host side of `env.AI.run()`.
//!
//! The harness maps a Workers AI input to an OpenAI-shaped body and names the
//! route. This op finds the endpoint, the key and the upstream model name in
//! the node's configuration (`crate::ai`), makes the call, and hands back the
//! answer. The key stays here: it is neither an argument nor a result.
//!
//! The endpoint is the operator's own, so the egress policy does not apply to
//! it, redirects are not followed, and the call leaves through its own client.
//! A call is a subrequest, and it waits on the output gate like a `fetch`.

use super::*;

thread_local! {
    static CLIENT: reqwest::Client = crate::tls_roots::apply(reqwest::Client::builder())
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
}

/// The most of an upstream error that reaches the Worker.
const ERROR_BODY_BYTES: usize = 2048;

/// `__ai_run(route, model, bodyJson, stream)`. Resolves to the upstream JSON
/// text, or, when `stream` is true, to the id of a body stream of the
/// upstream's server-sent events.
pub(super) fn op_ai_run(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    if let Err(error) = context.charge_subrequest() {
        return loader_throw(scope, &error);
    }
    let route = args.get(0).to_rust_string_lossy(scope);
    let model = args.get(1).to_rust_string_lossy(scope);
    let body = args.get(2).to_rust_string_lossy(scope);
    let stream = args.get(3).boolean_value(scope);
    let Some(route) = crate::ai::Route::parse(&route) else {
        return loader_throw(scope, "AI: unknown route");
    };
    let config = match crate::ai::load() {
        Ok(Some(config)) => config,
        Ok(None) => {
            return loader_throw(
                scope,
                &format!(
                    "Workers AI is not configured on this node; set {} and {}",
                    crate::ai::BASE_URL,
                    crate::ai::MODELS
                ),
            )
        }
        Err(error) => return loader_throw(scope, &format!("Workers AI: {error:#}")),
    };
    let request = match config.request(route, &model, &body) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &error),
    };
    let client = CLIENT.with(|client| client.clone());
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let stream_service = http_stream_service();
    let timeout = crate::env_vars::positive_or("CELLD_FETCH_TIMEOUT_S", 120)
        .expect("validated CELLD_FETCH_TIMEOUT_S");
    let id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let mut builder = client
            .post(&request.url)
            .timeout(std::time::Duration::from_secs(timeout))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(request.body);
        if stream {
            builder = builder.header(reqwest::header::ACCEPT, "text/event-stream");
        }
        if let Some(bearer) = request.bearer {
            let mut value = reqwest::header::HeaderValue::from_str(&bearer)
                .map_err(|_| "AI: the API key is not a valid header value".to_string())?;
            value.set_sensitive(true);
            builder = builder.header(reqwest::header::AUTHORIZATION, value);
        }
        let mut response = builder.send().await.map_err(|error| {
            // The error names the URL, which carries no credential.
            config.redact(&format!("AiError: the upstream request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            // A credential error says nothing the Worker can use, and an
            // upstream may echo the key it was sent.
            let detail = if matches!(status.as_u16(), 401 | 403) {
                String::new()
            } else {
                let limit = config.error_read_limit(ERROR_BODY_BYTES);
                let mut text = Vec::new();
                while text.len() < limit {
                    match response.chunk().await {
                        Ok(Some(chunk)) => text.extend_from_slice(&chunk),
                        _ => break,
                    }
                }
                text.truncate(limit);
                format!(": {}", config.error_detail(&text, ERROR_BODY_BYTES))
            };
            return Err(format!(
                "AiError: the upstream answered {}{detail}",
                status.as_u16()
            ));
        }
        if stream {
            return stream_service
                .register_source(HttpStreamSource::Response(response))
                .map(|stream_id| stream_id.to_string())
                .ok_or_else(|| format!("AI: {HTTP_STREAM_REGISTRATION_CLOSED}"));
        }
        response.text().await.map_err(|error| {
            config.redact(&format!("AiError: reading the upstream answer: {error}"))
        })
    });
    rv.set(promise_for(scope, id));
}

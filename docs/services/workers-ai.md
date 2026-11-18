# Workers AI

A Worker calls `env.AI.run(model, input, options)` as it does on Cloudflare.
celld has no models of its own: the node sends each call to an OpenAI-compatible
API that the operator configures, such as OpenAI, a gateway, or a local server.
Read the
[Workers AI documentation](https://developers.cloudflare.com/workers-ai/) for
the binding's API.

## Declare the binding

```jsonc
{
  "ai": { "binding": "AI" }
}
```

A deployment that declares `ai` needs a node that supports it. A node without
the endpoint configuration below still loads the Worker, and `run()` throws an
error that names the missing setting.

## Configure the node

| setting | meaning |
| --- | --- |
| `CELLD_AI_BASE_URL` | The API root, such as `https://api.openai.com/v1`. celld calls `<root>/chat/completions` and `<root>/embeddings`. It takes no credentials, query, or fragment |
| `CELLD_AI_MODELS` | A JSON object from a Workers AI model name to the endpoint's model name |
| `CELLD_AI_API_KEY_FILE` | A file that holds the bearer token. Prefer it to the variable |
| `CELLD_AI_API_KEY` | The bearer token itself. Set at most one of the two |

```sh
export CELLD_AI_BASE_URL=https://api.openai.com/v1
export CELLD_AI_API_KEY_FILE=/run/secrets/ai-key
export CELLD_AI_MODELS='{
  "@cf/meta/llama-3.1-8b-instruct": "gpt-4o-mini",
  "@cf/baai/bge-base-en-v1.5": "text-embedding-3-small"
}'
```

The key is optional, for a server that takes none. A blank variable counts as
unset. celld logs a warning when the key file can be read by other users. A malformed setting stops
the node at startup. The settings belong to the node, so a Worker cannot choose
another endpoint or model. `celld dev` reads the same settings.

The node makes the HTTP call, so the key never enters a Worker. celld does not
log it, and it removes the key from an upstream error before the Worker sees
it. A `401` or `403` from the endpoint reaches the Worker without the body.

A request body from the Worker and an answer from the endpoint may each be up
to 32 MiB. A larger request is refused without a call, and a larger answer
fails the call with an error that names the limit.

The endpoint is the operator's own, so the egress policy (`CELLD_EGRESS`) does
not apply to it and a private or loopback address works. celld does not follow
redirects from the endpoint. The call counts as a subrequest, and its time
limit is `CELLD_FETCH_TIMEOUT_S`.

## Text generation

The input is `messages` (objects with a string `role` and a string `content`)
or `prompt`. `max_tokens`, `temperature`, `top_p`, `seed`, `frequency_penalty`,
and `presence_penalty` pass through; other options are ignored.

```js
const { response, usage } = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
  messages: [{ role: "user", content: "Say hello" }],
});
```

The result is `{ response }`, with the endpoint's `usage` when it reports one.

With `stream: true` the result is a `ReadableStream` of server-sent events in
the Workers AI shape: one `data: {"response":"..."}` event for each piece of
text, then `data: [DONE]`.

```js
const stream = await env.AI.run(model, { prompt, stream: true });
return new Response(stream, { headers: { "content-type": "text/event-stream" } });
```

A stream that the endpoint cuts short, or that carries an error event, does not
end with `[DONE]`. The stream errors instead, and reading it throws an
`AiError` with the endpoint's message (without the key), so a Worker cannot
take a partial answer for a complete one.

## Embeddings

The input is `text`, a string or an array of strings. The result is
`{ shape, data }`, where `data` holds one vector for each text in input order
and `shape` is `[count, dimensions]`.

## Models

The input chooses the route, and the node's map chooses the model. Any
Workers AI name works once the operator maps it, so these families are the ones
the binding is built for:

- text generation, such as `@cf/meta/llama-3.1-8b-instruct`, mapped to a chat
  model
- text embeddings, such as `@cf/baai/bge-base-en-v1.5`, mapped to an embedding
  model

A model that is not in `CELLD_AI_MODELS` throws an error that names it, and the
node makes no call.

## Differences from Cloudflare

- The models are the ones behind the endpoint, so output, vector size, and
  limits differ from Cloudflare's models.
- Image, speech, translation, and other model families are not supported.
  A call whose input has none of `messages`, `prompt`, or `text` throws.
- Tool calls, `raw` prompts, LoRA, AI Gateway options, and `returnRawResponse`
  are not supported. `env.AI.models()` and `toMarkdown()` do not exist.
- A stream carries `usage` only when the endpoint sends it in the stream.

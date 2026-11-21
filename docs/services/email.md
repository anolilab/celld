# Email

The `send_email` binding hands a message to an SMTP server that the operator
configures for the node. The `email()` handler receives a message that the
operator's relay or webhook posts to the node. Read the Cloudflare documentation
for [sending](https://developers.cloudflare.com/email-service/api/send-emails/workers-api/)
and for the [`email()` handler](https://developers.cloudflare.com/email-service/api/route-emails/workers-api/).
celld is not a mail server: it does not listen on port 25 and does not look up
MX records. [Receiving mail](#receiving-mail) says how to connect one.

## Example

```jsonc
{
  "send_email": [
    { "name": "EMAIL" },
    { "name": "NOTIFY_OPS", "destination_address": "ops@example.com" }
  ]
}
```

```js
import { EmailMessage } from "cloudflare:email";

export default {
  async fetch(request, env) {
    // Fields, and celld builds the MIME message.
    const { messageId } = await env.EMAIL.send({
      from: "noreply@example.com",
      to: "bob@example.com",
      subject: "Welcome",
      text: "Thanks for signing up.",
      html: "<p>Thanks for signing up.</p>",
    });

    // Or a finished RFC 5322 message with its envelope.
    const raw =
      "From: noreply@example.com\r\nTo: ops@example.com\r\nSubject: Hi\r\n\r\nHello\r\n";
    await env.NOTIFY_OPS.send(new EmailMessage("noreply@example.com", "ops@example.com", raw));
    return new Response(messageId);
  },
};
```

## Configuration of the node

The relay belongs to the node, not to a Worker. A Worker never sees its
address or credentials: they are not in `env` or `process.env`, in an error a
Worker reads, or in a log.

| Variable | Meaning |
| --- | --- |
| `CELLD_SMTP_HOST` | The relay's name or IP address. Without it the node has no relay, and `send()` rejects with `E_INTERNAL_SERVER_ERROR` |
| `CELLD_SMTP_PORT` | Its port. The default is 587 for `starttls`, 465 for `tls` and 25 for `off` |
| `CELLD_SMTP_TLS` | `starttls` (default), `tls` or `off` |
| `CELLD_SMTP_USERNAME` | The account. Set it with a password |
| `CELLD_SMTP_PASSWORD` | The password |
| `CELLD_SMTP_PASSWORD_FILE` | A file that holds the password, in place of `CELLD_SMTP_PASSWORD`. Setting both is an error. celld reads the file with the settings, accepts a symbolic link as a mounted secret is, drops one trailing line end, and warns (`smtp_password_file_shared`) when other users can read it |
| `CELLD_SMTP_HELO_NAME` | The name celld gives in `EHLO`. Default `localhost` |

The node checks these at start and refuses to start on a malformed value, or
on a variable that is set while `CELLD_SMTP_HOST` is not. The error names the
variable and never the password. That includes a password file that cannot be
read. The warning about a shared file is logged once, when the node reads the file at start.

- **TLS is required unless `CELLD_SMTP_TLS=off` says otherwise.** With
  `starttls`, a relay that does not offer `STARTTLS` is an error: celld never
  continues in the clear and never logs in before TLS. The relay's certificate
  must chain to the bundled public roots. `CELLD_EXTRA_CA_CERTS` is for what
  Workers dial and does not apply here.
- Login uses `AUTH PLAIN`, or `AUTH LOGIN` when the relay offers only that.
  A user name and password too long for one command line (512 bytes) are sent
  after the relay's `334`, as RFC 4954 allows.
  With `off` the login is sent in the clear: use it for a relay on the same
  host or a trusted network.
- The host makes the connection. A private or loopback address is fine on every
  node, a fleet node included, as a relay on the same network or a mail agent
  on the same machine is usual: the operator names the relay, and a Worker
  cannot. An unspecified, link-local,
  cloud metadata, multicast or reserved address is refused, so is a listener of
  this node, and the connection goes only to the addresses that passed. This
  holds in `celld dev` and with `CELLD_EGRESS=open`.
- A relay's reply text is never passed to a Worker or logged, because a relay
  can echo what it was sent. A failure reads `E_DELIVERY_FAILED` with the stage
  and the reply code, such as `the SMTP server answered 535 at auth`.

A project with no `main` (assets only) cannot declare `send_email`.

A `send_email` deployment requires the `send-email-v1` feature, so a node that
does not have the binding refuses it. A node that has the binding but no relay
loads the Worker and fails each `send()`.

## Restrictions

A binding takes Cloudflare's three optional restrictions, and refuses any other
key:

- `destination_address`: the binding can send only to this address. A
  `send({ ... })` without `to`, `cc` or `bcc` sends to it.
- `allowed_destination_addresses`: the binding can send only to these
  addresses. It cannot be combined with `destination_address`.
- `allowed_sender_addresses`: the binding can send only from these addresses.

Addresses are compared exactly as written, including case. celld checks every
recipient, `cc` and `bcc` included, and the sender before it connects to the
relay. A refusal rejects with `E_RECIPIENT_NOT_ALLOWED` or
`E_SENDER_NOT_VERIFIED`, and no connection is made. Addresses must be plain
ASCII `local@domain`: no display name, quoted form, space or angle bracket.

## The two forms of `send()`

**`send({ from, to, subject, text, html, ... })`** takes `from`, `to`, `cc`,
`bcc`, `replyTo` (a string or `{ name, email }`), `subject`, `text`, `html`,
`headers` and `attachments`. At least one of `to`, `cc` and `bcc` is required
(unless the binding has a `destination_address`), and at least one of `text`
and `html`. celld builds the headers, a `Date`, a `Message-ID`, and the MIME
structure. A `bcc` recipient is in the envelope and never in the message. A
field that is not in this list is refused.

Custom `headers` follow Cloudflare's rules: any `X-` header, and the allowlisted
headers of the [headers reference](https://developers.cloudflare.com/email-service/reference/headers/)
such as `In-Reply-To`, `References` and `List-Unsubscribe`. celld checks the
name, that the value is not empty, has no line break and is at most 2,048
bytes, at most 20 headers besides `X-` ones, and 16 KiB in all. It does not
check the syntax of the value of an allowlisted header, such as the URI of
`List-Unsubscribe`. A value with a non-ASCII character is encoded as RFC 2047
words only in a free-text header: any `X-` header, `Comments`, `Organization`
and `Thread-Topic`. Every other allowlisted header holds addresses, URIs, ids
or tokens, where an encoded word is not valid, so a non-ASCII value is refused
with `E_HEADER_VALUE_INVALID`.

**`send(new EmailMessage(from, to, raw))`** takes the envelope and a finished
message as a string or a `ReadableStream` of bytes. celld requires that

- the message has exactly one `From:` header whose address is `from`, so an
  `allowed_sender_addresses` list also holds for what a recipient sees. The
  header is `addr`, `"Display Name" <addr>` or `plain words <addr>`, with
  nothing after the `>`. A comment, a backslash or any other text is refused,
  because mail programs disagree about where a comment ends;
- a `Sender:`, `Resent-From:` or `Resent-Sender:` header, if there is one, has
  that same single address too; any other address is refused;
- it has no `Received:` header;
- every line end is CRLF: a bare LF or CR is changed to CRLF.

celld replaces the `Message-ID` header with one it makes and drops the `Bcc:` and
`Resent-Bcc:` headers. `send()` resolves to `{ messageId }` in both forms.

## Limits

- A message weighs at most 25 MiB, attachments included. celld stops reading a
  stream at the limit and rejects with `E_CONTENT_TOO_LARGE`. A stream has 60
  seconds to yield the whole message; after that it is cancelled and `send()`
  rejects with `E_TIMEOUT`. A message given
  as fields is also refused when its call, as JSON, is more than about 25 MiB.
- At most 50 recipients, `to`, `cc` and `bcc` together, and 32 attachments.
- The header section of a raw message is at most 64 KiB, and a header line at
  most 998 bytes. A line of a raw body is at most 998 bytes too, and a longer
  one is refused with `E_LINE_TOO_LONG`: celld does not fold a body. A subject
  has at most 998 characters.
- The size is counted in bytes of the message as sent, not in characters: a
  string of 14 million characters of two bytes each is over the limit, and
  rejects with `E_CONTENT_TOO_LARGE` like any other message that is too large.
- Each address is at most 254 bytes, and each list in the configuration holds
  at most 1,000 addresses.
- A relay that announces a `SIZE` smaller than the message is not sent the
  message.
- A relay has 60 seconds to answer one command and 60 seconds for each 64 KiB
  chunk of the body it is sent, then 10 minutes to answer the end of the body,
  as it may scan the message first. A whole transaction may take five minutes,
  plus those 10 minutes, plus the time the body needs at 64 KiB a second.
- A call costs a subrequest, as a `fetch()` does. An isolate has at most four
  messages in flight: the call after that rejects at once with
  `E_RATE_LIMIT_EXCEEDED`, before a stream is read from. The slot of a call
  that is waiting on a stream goes back when the event ends, so a stream that
  never yields does not keep it. A call past the invocation's subrequest limit
  rejects with `E_RATE_LIMIT_EXCEEDED` too. A script has at most four
  transactions running on a node, and a node runs at most 16. A call beyond
  either waits up to 60 seconds, and then rejects with `E_RATE_LIMIT_EXCEEDED`
  (for a script) or `E_DELIVERY_FAILED` (for the node).

Each `send()` is one SMTP transaction on a new connection: `EHLO`, `STARTTLS`,
`AUTH`, `MAIL FROM`, one `RCPT TO` for each recipient, `DATA` with dot-stuffing,
and `QUIT`. `send()` resolves when the relay has accepted the message: `QUIT`
is written after that and not waited for. A relay that refuses one recipient
fails the whole message.

## 8-bit and non-ASCII content

A message built from fields is plain ASCII on the wire: the body is base64, and
a non-ASCII subject, name or file name is encoded. A raw message may hold any
bytes. If it has a byte of 128 or more, `MAIL FROM` carries `BODY=8BITMIME`, and
if a header has one it carries `SMTPUTF8` too. A relay that does not offer what
the message needs is not sent it: `send()` rejects with `E_8BIT_NOT_SUPPORTED`
before any message is started. Encode the message as 7-bit (quoted-printable or
base64) for such a relay.

## Error codes

`E_VALIDATION_ERROR`, `E_FIELD_MISSING`, `E_RECIPIENT_NOT_ALLOWED`,
`E_SENDER_NOT_VERIFIED`, `E_TOO_MANY_RECIPIENTS`, `E_TOO_MANY_ATTACHMENTS`,
`E_HEADER_NAME_INVALID`, `E_HEADER_NOT_ALLOWED`, `E_HEADER_USE_API_FIELD`,
`E_HEADER_VALUE_INVALID`, `E_HEADERS_TOO_MANY`, `E_HEADERS_TOO_LARGE`,
`E_CONTENT_TOO_LARGE`, `E_LINE_TOO_LONG`, `E_8BIT_NOT_SUPPORTED`,
`E_RATE_LIMIT_EXCEEDED`, `E_TIMEOUT`, `E_DELIVERY_FAILED` and `E_INTERNAL_SERVER_ERROR` (no
relay, a relay setting that is invalid, or a send that was not made because the
writes the handler made before it could not be made durable). celld
connects only after those writes are durable, as it does for `fetch()`.

## Receiving mail

A Worker that exports `email(message, env, ctx)` handles mail that the operator
posts to the node. The operator's mail server, a forwarding rule or a provider's
webhook sends each message to the node's internal listener; celld turns it into
one `email()` event on the primary script, the one that serves `fetch`.

```js
export default {
  async email(message, env, ctx) {
    if (Number(message.headers.get("x-spam-score")) > 5) {
      message.setReject("Message appears to be spam");
      return;
    }
    await message.forward("inbox@example.com");
  },
};
```

### The route

```
POST /email/inbound?from=<envelope sender>&to=<envelope recipient>
Authorization: Bearer <inbound token or operator token>

<the raw RFC 5322 message>
```

- It is on the **internal listener**, which refuses every request that does not
  carry the node's [operator token](../security.md#require-an-operator-token),
  before any route is looked up. It is not on the public listener, which hands
  the same path to the Worker's `fetch()`. A Worker cannot reach it: egress to a
  node's own listeners is refused, and it has no token.
- The operator token opens every route of the internal listener, so give the
  relay a token of its own. `CELLD_EMAIL_INBOUND_TOKEN` (or a file named by
  `CELLD_EMAIL_INBOUND_TOKEN_FILE`) sets one that this route accepts and every
  other route refuses with 401. The operator token keeps working for the
  route; without either setting, the route needs the operator token.
- The body is the message, bytes as received, with any line ends. It is not
  rewritten: `message.raw` is what was posted. A `Received:` header, a `From:`
  that is not the envelope sender and a `Message-ID` are all normal.
- `from` is the envelope sender (`MAIL FROM`) and `to` the envelope recipient
  (`RCPT TO`), percent-encoded, and both are required: a request without one
  is refused with 400. They are never read from the message's headers, because
  `reply()` sends from the domain of `to`, and a header is the sender's to
  write. `from` may be empty for a bounce (`MAIL FROM:<>`). Each is a plain
  ASCII `local@domain`, as for `send()`. celld does not check `to` against a
  list of domains it receives for; the relay decides which mail it posts.
- One request is one message and one `email()` call; a message for several
  recipients is posted once for each. At most four messages are
  in flight; a message keeps its place until its handler's event has ended. A
  handler that is cancelled at the deadline or whose relay hung up ends, and
  gives its place back, as soon as it unwinds.
- `forward()` and `reply()` need `CELLD_SMTP_HOST`; without a relay they
  reject, and a handler that does not catch it answers 500 and is retried.
- Each message is held in memory twice while it is handled: the host's copy,
  which `forward()` reads, and the Worker's copy of `message.raw`. The host's
  copy is the one the request carried and is not copied again. A `forward()`
  builds one more, briefly, while it sends.
- A header line over 998 bytes or a NUL byte anywhere in the message is refused
  with 400, so the relay bounces it, even when the server in front accepted it.
- The poster must be the operator's own relay or webhook. Give it
  `CELLD_EMAIL_INBOUND_TOKEN`, not the operator token, which also opens
  `/shutdown` and `/reload`. Put a small proxy that adds the header in front of
  a provider that cannot send one, and keep the node's internal listener off the
  internet.
- celld does not check SPF, DKIM or DMARC. Check them at the relay, and have it
  put its result in an `Authentication-Results` header for the Worker to read.
  Mail that fails can be refused there, or by `setReject()`.

### What the Worker sees

`message` has the shape Cloudflare documents:

| | |
| --- | --- |
| `from`, `to` | The envelope, as posted |
| `headers` | A `Headers` of every header. A header that `Headers` cannot hold (a name with a separator such as `(`) is left out; `raw` has it |
| `raw` | A `ReadableStream` of the posted bytes |
| `rawSize` | Their length |
| `canBeForwarded` | Always `true` |
| `setReject(reason)` | Refuses the message permanently |
| `forward(rcptTo, headers?)` | Sends the message on |
| `reply(message)` | Sends a reply to the sender |

`setReject()`, `forward()` and `reply()` work until the handler returns; after
that they throw, so a message that is kept cannot bounce mail that was accepted.

### The answer

The handler's outcome is the response, so the poster can accept, bounce or
retry:

| Status | Body | Meaning |
| --- | --- | --- |
| 200 | `{"ok":true,"outcome":"accepted"}` | The handler returned. Accept the message |
| 550 | `{"ok":false,"outcome":"rejected","reason":"..."}` | The handler called `setReject()`. Bounce with a permanent failure and `reason` (printable ASCII, at most 200 characters, so it is safe in an SMTP reply line) |
| 500 | `{"ok":false,"outcome":"error","error":"..."}` | The handler threw. Treat it as temporary and retry |
| 501 | `{"ok":false,"outcome":"no_handler",...}` | The primary Worker has no `email()` handler. Treat it as temporary: the operator can deploy one |
| 504 | | The handler did not finish within 60 seconds (`CELLD_EMAIL_INBOUND_DEADLINE_S`). The event is cancelled, as one whose client hung up is: it gives its place back, and a `forward()` or `reply()` it makes later is refused with `E_TIMEOUT`. Temporary, and the retry runs the handler again |
| 408 | | The message was not received within the same time. Temporary |
| 503 | | The node is busy (at most four messages at once, with `Retry-After`), or has no Worker runtime. Temporary |
| 400, 413, 431 | `{"ok":false,"error":"..."}` | The message was refused: empty, malformed, with a NUL byte, over 25 MiB (413), with a header line over 998 bytes, or with more than 64 KiB or 1,000 headers (431). The error names the fault |
| 401 | | No or a wrong operator token |

200 accepts and 550 bounces. 400, 413 and 431 mean the message itself is bad and
will not improve on a retry, so bounce it too. Every other status is temporary:
retry later. The message body is never logged.

### Wiring a relay

Any program that can make an HTTP request per message will do. With Postfix, a
`pipe` transport runs a script for each message; the script posts it:

```sh
#!/bin/sh
# usage: deliver-to-celld <sender> <recipient>   (needs curl 7.87 or later)
# CELLD_INTERNAL_URL is the node's internal listener, e.g. http://127.0.0.1:8081.
# CELLD_INBOUND_HEADER is a file that only the relay's user can read, with the
# line `Authorization: Bearer <token>`. curl reads the header from it, so the
# token is not in the process list as it is after -H "Authorization: ...".
status=$(curl -s -o /dev/null -w '%{http_code}' \
  -H "@$CELLD_INBOUND_HEADER" \
  --data-binary @- \
  --url-query "from=$1" --url-query "to=$2" \
  "$CELLD_INTERNAL_URL/email/inbound")
[ "$status" = 200 ] && exit 0      # accepted
[ "$status" = 550 ] && exit 67     # bounce: a permanent failure
exit 75                            # EX_TEMPFAIL: try again later
```

A provider's inbound webhook that posts the raw message works the same way
through a proxy that adds the `Authorization` header. A provider that posts a
parsed form needs a proxy that rebuilds the message.

### `forward()` and `reply()`

Both deliver through the [operator's relay](#configuration-of-the-node), with
the checks and limits of `send()`: plain ASCII addresses, CRLF line ends, no
`Bcc:`, a message of at most 25 MiB with lines of at most 998 bytes, a
subrequest each, and at most four messages in flight for an isolate. They are
not restricted by any `send_email` binding. `forward()` has a list of its own.

- `forward(rcptTo, headers?)` sends the message as received to `rcptTo`, which
  must be on the node's `CELLD_EMAIL_FORWARD_ALLOW` list: whole addresses,
  compared exactly as written, and `@domain` entries, which match that domain
  and not its subdomains. Any other address is refused with
  `E_RECIPIENT_NOT_ALLOWED`, as Cloudflare forwards only to verified
  destinations, and with the list unset `forward()` is refused. The envelope
  sender is the address that received the message (`to`), not the sender of the
  message, whose domain would not accept the relay's mail; the `From:` header
  stays. celld does not rewrite the sender as SRS does, so a destination that
  checks SPF sees the receiving domain's policy. The message's bytes are kept
  (`Received:`, `Message-ID` and signature headers included) apart from CRLF
  line ends and the loss of `Bcc:` and `Resent-Bcc:`. A message with no usable
  `Message-ID` gets one, and the id `forward()` resolves with is that one.
  Only headers whose name starts with `X-` are added; others in `headers` are
  dropped, and a value that is not ASCII, is empty, holds a control character
  other than a tab or is longer than 2,048 bytes is refused with
  `E_HEADER_VALUE_INVALID`, as it is in `send()`. At most 20 are added. A
  message with a body line over 998 bytes is refused with `E_LINE_TOO_LONG`.
- `reply(message)` takes a builder object (`from`, `subject`, `text`, `html`,
  `replyTo`, `attachments`, `headers`) or an `EmailMessage`. The reply goes to
  the sender of the incoming message: a builder has no `to`, `cc` or `bcc`, and
  an `EmailMessage` must be addressed to the incoming `from`. It must be sent
  from the domain of the incoming `to`. celld sets `In-Reply-To` and
  `References` from the incoming `Message-ID`; a value the Worker sets must be
  the same. It is refused when the incoming message has no `Message-ID`
  (`E_FIELD_MISSING`), more than 100 `References`, or no sender (a bounce). A
  message may be replied to once, and a failed reply counts, but one turned away
  for lack of a slot or a subrequest (`E_RATE_LIMIT_EXCEEDED`) does not. A reply
  resolves to `{ messageId }` and rejects with the codes of `send()`. It is
  refused with `E_RECIPIENT_NOT_ALLOWED` when the incoming message is automatic
  mail (`Auto-Submitted` with a value other than `no`, or `Precedence: bulk`,
  `list` or `junk`) or comes from `MAILER-DAEMON` or `postmaster`, so two
  machines cannot answer each other for ever. celld does not check DMARC of the
  incoming message, which Cloudflare requires before it allows a reply; check
  it at the relay.

## Differences from Cloudflare

- Delivery is the operator's relay, not Cloudflare's network. Sender domains,
  suppression lists, daily quotas and the account's verified addresses do not
  exist here; the relay and the binding's lists decide.
- Mail does not arrive by itself. The operator posts it to
  [`/email/inbound`](#receiving-mail); there is no Email Routing, no address
  rules and no verified destination list. `forward()` goes to the addresses of
  `CELLD_EMAIL_FORWARD_ALLOW`, which stands in for the verified destinations.
- `reply()` does not check DMARC: the operator's relay decides which mail may
  reach the Worker. The `E_TEMPORARY_AUTHENTICATION_ERROR` code is never raised.
- The event runs on the primary script only, and is not routed by recipient.
- A message is sent when `send()` resolves, once the relay has accepted it. That
  is not a delivery to the recipient.
- `E_DAILY_LIMIT_EXCEEDED`, `E_ACCOUNT_SUSPENDED`, `E_RECIPIENT_SUPPRESSED`
  and `E_SENDER_DOMAIN_NOT_AVAILABLE` are never raised. `E_RATE_LIMIT_EXCEEDED`
  means the in-flight limits above, not an account quota.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.

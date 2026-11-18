# Email

The `send_email` binding hands a message to an SMTP server that the operator
configures for the node. Read the
[Cloudflare Email Service documentation](https://developers.cloudflare.com/email-service/api/send-emails/workers-api/)
for the API. celld does not receive mail: the `email()` handler is not
available.

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
| `CELLD_SMTP_PASSWORD_FILE` | A file that holds the password, in place of `CELLD_SMTP_PASSWORD`. Setting both is an error. celld reads the file at the first `send()`, accepts a symbolic link as a mounted secret is, drops one trailing line end, and warns (`smtp_password_file_shared`) when other users can read it |
| `CELLD_SMTP_HELO_NAME` | The name celld gives in `EHLO`. Default `localhost` |

The node checks these at start and refuses to start on a malformed value, or
on a variable that is set while `CELLD_SMTP_HOST` is not. The error names the
variable and never the password. A password file that cannot be read is found
at the first `send()`, which logs `smtp_settings_invalid` once with the cause.

- **TLS is required unless `CELLD_SMTP_TLS=off` says otherwise.** With
  `starttls`, a relay that does not offer `STARTTLS` is an error: celld never
  continues in the clear and never logs in before TLS. The relay's certificate
  must chain to the bundled public roots. `CELLD_EXTRA_CA_CERTS` is for what
  Workers dial and does not apply here.
- Login uses `AUTH PLAIN`, or `AUTH LOGIN` when the relay offers only that.
  With `off` the login is sent in the clear: use it for a relay on the same
  host or a trusted network.
- The host makes the connection. A private or loopback address is fine, as a
  relay on the same network or machine is usual. An unspecified, link-local,
  cloud metadata, multicast or reserved address is refused, so is a listener of
  this node, and the connection goes only to the addresses that passed. This
  holds in `celld dev` and with `CELLD_EGRESS=open`.
- A relay's reply text is never passed to a Worker or logged, because a relay
  can echo what it was sent. A failure reads `E_DELIVERY_FAILED` with the stage
  and the reply code, such as `the SMTP server answered 535 at auth`.

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
`List-Unsubscribe`.

**`send(new EmailMessage(from, to, raw))`** takes the envelope and a finished
message as a string or a `ReadableStream` of bytes. celld requires that

- the message has exactly one `From:` header whose address is `from`, so an
  `allowed_sender_addresses` list also holds for what a recipient sees;
- it has no `Received:` header;
- every line end is CRLF: a bare LF or CR is changed to CRLF.

celld replaces the `Message-ID` header with one it makes and drops a `Bcc:`
header. `send()` resolves to `{ messageId }` in both forms.

## Limits

- A message weighs at most 25 MiB, attachments included. celld stops reading a
  stream at the limit and rejects with `E_CONTENT_TOO_LARGE`.
- At most 50 recipients, `to`, `cc` and `bcc` together, and 32 attachments.
- The header section of a raw message is at most 64 KiB, and a header line at
  most 998 bytes. A subject has at most 998 characters.
- Each address is at most 254 bytes, and each list in the configuration holds
  at most 1,000 addresses.
- A relay that announces a `SIZE` smaller than the message is not sent the
  message.
- A transaction takes at most five minutes, and a relay has 60 seconds to answer
  one command. A node runs at most 16 transactions at once, and a call beyond
  that waits.

Each `send()` is one SMTP transaction on a new connection: `EHLO`, `STARTTLS`,
`AUTH`, `MAIL FROM`, one `RCPT TO` for each recipient, `DATA` with dot-stuffing,
and `QUIT`. A relay that refuses one recipient fails the whole message. celld
connects only after the writes the handler made so far are durable, as it does
for `fetch()`.

## Differences from Cloudflare

- Delivery is the operator's relay, not Cloudflare's network. Sender domains,
  suppression lists, daily quotas and the account's verified addresses do not
  exist here; the relay and the binding's lists decide.
- There is no `email()` handler, no `message.forward()` and no
  `message.reply()`.
- A message is sent when `send()` resolves, once the relay has accepted it. That
  is not a delivery to the recipient.
- `E_RATE_LIMIT_EXCEEDED`, `E_DAILY_LIMIT_EXCEEDED`, `E_ACCOUNT_SUSPENDED`,
  `E_RECIPIENT_SUPPRESSED` and `E_SENDER_DOMAIN_NOT_AVAILABLE` are never raised.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.

// An email() handler whose behaviour the message picks with its X-Mode header.
//   echo        rejects with what the handler saw, so a test can read it back
//   reject      setReject with a reason that holds a line break
//   forward     forwards to team@example.net with one X- header and one other
//   reply       replies with a builder message
//   reply-raw   replies with a raw EmailMessage
//   reply-twice replies twice and reports the second error
//   reply-wrong replies to someone else and reports the error
//   reply-code  replies with a builder message and reports "sent" or "code:<code>"
//   forward-to  forwards to the address in X-To and reports "sent" or "code:<code>"
//   forward-id  forwards to team@example.net and reports the messageId it got
//   reply-limit fills the isolate's send slots with forwards, replies (refused for
//               lack of a slot), waits for the forwards, replies again, and
//               reports both outcomes
//   forward-throw  forwards to team@example.net, then throws
//   raw-reply-twice  replies with a streamed raw EmailMessage twice (the second is refused), then
//               forwards four times at once and reports the second error and the four outcomes
//   forward-hang  forwards to team@example.net and then waits for an hour
//   forward-wait-throw  starts a forward in waitUntil, then throws
//   hang        waits for an hour
//   hang-forward  waits for an hour, and tries to forward from waitUntil after 1.5 s
//   forward-then-reject  forwards to team@example.net, then calls setReject
//   wait-reply-then-reject  starts a streamed raw reply in waitUntil (its bytes arrive after
//               0.6 s, when the route has answered), then calls setReject at once
//   wait-reply-then-throw  the same, then throws
//   nested-missing  calls an entrypoint of this script that has no fetch()
//   late        tries to reject the message after the handler returned
//   throw       throws
//   (any other) accepts
import { EmailMessage } from "cloudflare:email";
import { WorkerEntrypoint } from "cloudflare:workers";

// An entrypoint with no fetch(), for a call the email handler makes.
export class NoFetch extends WorkerEntrypoint {}

// A handler that reports what it saw does so through setReject, and also keeps the
// report for GET /__report: a message the handler sent mail for answers 200 whatever
// it called, so its status carries no report.
let lastReport = "";
function report(message, text) {
  lastReport = text;
  message.setReject(text);
}

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/__report") return new Response(lastReport);
    if (path === "/__ready") return new Response("ready");
    return new Response("fetch");
  },

  async email(message, env, ctx) {
    lastReport = "";
    const subject = message.headers.get("subject") ?? "";
    switch (message.headers.get("x-mode")) {
      case "echo": {
        const text = await new Response(message.raw).text();
        message.setReject(
          [
            message.from,
            message.to,
            subject,
            message.rawSize,
            text.length,
            message.canBeForwarded,
            [...message.headers].length,
          ].join("|"),
        );
        return;
      }
      case "reject":
        message.setReject("no thanks\r\n250 ok é");
        return;
      case "forward": {
        const extra = new Headers();
        extra.set("X-Processed-By", "worker");
        extra.set("Subject", "changed");
        await message.forward("team@example.net", extra);
        return;
      }
      case "reply":
        await message.reply({ from: message.to, subject: "Re: " + subject, text: "thanks" });
        return;
      case "reply-raw":
        await message.reply(
          new EmailMessage(
            message.to,
            message.from,
            `From: ${message.to}\r\nTo: ${message.from}\r\nSubject: Re: ${subject}\r\n\r\nraw reply\r\n`,
          ),
        );
        return;
      case "reply-twice": {
        const reply = { from: message.to, subject: "Re", text: "once" };
        await message.reply(reply);
        try {
          await message.reply(reply);
        } catch (error) {
          report(message, "second:" + error.code);
        }
        return;
      }
      case "reply-wrong":
        try {
          await message.reply(
            new EmailMessage(message.to, "eve@example.net", "From: " + message.to + "\r\n\r\nx\r\n"),
          );
        } catch (error) {
          report(message, "code:" + error.code);
        }
        return;
      case "reply-code":
        try {
          await message.reply({ from: message.to, subject: "Re", text: "x" });
          report(message, "sent");
        } catch (error) {
          report(message, "code:" + error.code);
        }
        return;
      case "forward-to":
        try {
          await message.forward(message.headers.get("x-to"));
          report(message, "sent");
        } catch (error) {
          report(message, "code:" + error.code);
        }
        return;
      case "forward-id": {
        const { messageId } = await message.forward("team@example.net");
        report(message, "id:" + messageId);
        return;
      }
      case "forward-throw":
        await message.forward("team@example.net");
        throw new Error("after the forward");
      case "raw-reply-twice": {
        // A stream, so each reply reserves its slot before the host sees it.
        const raw = () =>
          new EmailMessage(
            message.to,
            message.from,
            new ReadableStream({
              start(controller) {
                controller.enqueue(
                  new TextEncoder().encode(
                    `From: ${message.to}\r\nTo: ${message.from}\r\nSubject: Re\r\n\r\nraw reply\r\n`,
                  ),
                );
                controller.close();
              },
            }),
          );
        await message.reply(raw());
        const second = await message.reply(raw()).then(() => "ok", (error) => error.code);
        const forwards = await Promise.all(
          [1, 2, 3, 4].map(() => message.forward("team@example.net").then(() => "ok", (error) => error.code)),
        );
        report(message, [second, ...forwards].join("|"));
        return;
      }
      case "reply-limit": {
        const reply = { from: message.to, subject: "Re", text: "x" };
        const forwards = [1, 2, 3, 4].map(() => message.forward("team@example.net"));
        const first = await message.reply(reply).then(() => "ok", (error) => error.code);
        await Promise.all(forwards);
        const second = await message.reply(reply).then(() => "ok", (error) => error.code);
        report(message, first + "|" + second);
        return;
      }
      case "forward-wait-throw":
        ctx.waitUntil(message.forward("team@example.net"));
        throw new Error("after starting the forward");
      case "forward-hang":
        await message.forward("team@example.net");
        await new Promise((resolve) => setTimeout(resolve, 3_600_000));
        return;
      case "hang-forward":
        ctx.waitUntil(
          new Promise((resolve) => setTimeout(resolve, 1500)).then(
            () => message.forward("team@example.net"),
            () => {},
          ).catch(() => {}),
        );
        await new Promise((resolve) => setTimeout(resolve, 3_600_000));
        return;
      case "hang":
        await new Promise((resolve) => setTimeout(resolve, 3_600_000));
        return;
      case "forward-then-reject":
        await message.forward("team@example.net");
        message.setReject("after the forward");
        return;
      case "wait-reply-then-reject":
        ctx.waitUntil(
          message.reply(
            new EmailMessage(
              message.to,
              message.from,
              new ReadableStream({
                async start(controller) {
                  await new Promise((resolve) => setTimeout(resolve, 600));
                  controller.enqueue(
                    new TextEncoder().encode(
                      `From: ${message.to}\r\nTo: ${message.from}\r\nSubject: Re\r\n\r\nraw reply\r\n`,
                    ),
                  );
                  controller.close();
                },
              }),
            ),
          ),
        );
        message.setReject("after starting the reply");
        return;
      case "wait-reply-then-throw":
        ctx.waitUntil(
          message.reply(
            new EmailMessage(
              message.to,
              message.from,
              new ReadableStream({
                async start(controller) {
                  await new Promise((resolve) => setTimeout(resolve, 600));
                  controller.enqueue(
                    new TextEncoder().encode(
                      `From: ${message.to}\r\nTo: ${message.from}\r\nSubject: Re\r\n\r\nraw reply\r\n`,
                    ),
                  );
                  controller.close();
                },
              }),
            ),
          ),
        );
        throw new Error("after starting the reply");
      case "nested-missing":
        await ctx.exports.NoFetch.fetch("http://nested/");
        return;
      case "late":
        ctx.waitUntil(
          new Promise((resolve) => setTimeout(resolve, 100)).then(() => {
            try {
              message.setReject("too late");
            } catch {}
          }),
        );
        return;
      case "throw":
        throw new Error("boom");
      default:
        return;
    }
  },
};

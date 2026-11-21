// An email() handler whose behaviour the message picks with its X-Mode header.
//   echo        rejects with what the handler saw, so a test can read it back
//   reject      setReject with a reason that holds a line break
//   forward     forwards to team@example.net with one X- header and one other
//   reply       replies with a builder message
//   reply-raw   replies with a raw EmailMessage
//   reply-twice replies twice and reports the second error
//   reply-wrong replies to someone else and reports the error
//   late        tries to reject the message after the handler returned
//   throw       throws
//   (any other) accepts
import { EmailMessage } from "cloudflare:email";

export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/__ready") return new Response("ready");
    return new Response("fetch");
  },

  async email(message, env, ctx) {
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
          message.setReject("second:" + error.code);
        }
        return;
      }
      case "reply-wrong":
        try {
          await message.reply(
            new EmailMessage(message.to, "eve@example.net", "From: " + message.to + "\r\n\r\nx\r\n"),
          );
        } catch (error) {
          message.setReject("code:" + error.code);
        }
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

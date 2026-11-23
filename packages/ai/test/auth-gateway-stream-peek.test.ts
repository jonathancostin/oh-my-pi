import { describe, expect, it } from "bun:test";
import { peekStream, withRetryAfter } from "@oh-my-pi/pi-ai/auth-gateway/stream-peek";
import { encodeStream } from "@oh-my-pi/pi-ai/providers/anthropic-messages-server";
import type { AssistantMessage, AssistantMessageEvent } from "@oh-my-pi/pi-ai/types";
import { AssistantMessageEventStream } from "@oh-my-pi/pi-ai/utils/event-stream";

function message(overrides: Partial<AssistantMessage> = {}): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "claude-test",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: 0,
		...overrides,
	} as AssistantMessage;
}

function streamOf(events: AssistantMessageEvent[], delayMs = 0): AssistantMessageEventStream {
	const s = new AssistantMessageEventStream();
	setTimeout(() => {
		for (const ev of events) s.push(ev);
		s.end();
	}, delayMs);
	return s;
}

function rateLimited(): AssistantMessage {
	return message({
		stopReason: "error",
		errorStatus: 429,
		errorMessage: "429 rate_limit_error: This request would exceed your rate limit. retry-after-ms=30000",
	});
}

async function drain(events: AssistantMessageEventStream): Promise<string[]> {
	const types: string[] = [];
	for await (const ev of events) types.push(ev.type);
	return types;
}

describe("auth-gateway peekStream", () => {
	it("reports an upstream 429 that arrives before any content", async () => {
		const failed = rateLimited();
		const peeked = await peekStream(
			streamOf([
				{ type: "start", partial: message() },
				{ type: "error", reason: "error", error: failed },
			]),
		);
		expect(peeked.kind).toBe("error");
		if (peeked.kind !== "error") return;
		expect(peeked.classified.status).toBe(429);
		expect(peeked.classified.type).toBe("rate_limit_error");
		expect(peeked.retryAfterMs).toBe(30000);
	});

	it("passes a healthy stream through with every event, in order", async () => {
		const done = message();
		const peeked = await peekStream(
			streamOf([
				{ type: "start", partial: message() },
				{ type: "text_start", contentIndex: 0, partial: message() },
				{ type: "text_delta", contentIndex: 0, delta: "hi", partial: message() },
				{ type: "text_end", contentIndex: 0, content: "hi", partial: message() },
				{ type: "done", reason: "stop", message: done },
			]),
		);
		expect(peeked.kind).toBe("stream");
		if (peeked.kind !== "stream") return;
		expect(await drain(peeked.events)).toEqual(["start", "text_start", "text_delta", "text_end", "done"]);
		expect(await peeked.events.result()).toBe(done);
	});

	it("keeps a failure after content inside the stream", async () => {
		const peeked = await peekStream(
			streamOf([
				{ type: "start", partial: message() },
				{ type: "text_start", contentIndex: 0, partial: message() },
				{ type: "error", reason: "error", error: rateLimited() },
			]),
		);
		expect(peeked.kind).toBe("stream");
		if (peeked.kind !== "stream") return;
		expect(await drain(peeked.events)).toEqual(["start", "text_start", "error"]);
	});

	it("commits to the stream when the first event is slower than the peek window", async () => {
		const peeked = await peekStream(
			streamOf(
				[
					{ type: "start", partial: message() },
					{ type: "done", reason: "stop", message: message() },
				],
				50,
			),
			undefined,
			10,
		);
		expect(peeked.kind).toBe("stream");
		if (peeked.kind !== "stream") return;
		expect(await drain(peeked.events)).toEqual(["start", "done"]);
	});
});

describe("auth-gateway retry-after", () => {
	it("rounds the upstream delay up to whole seconds", () => {
		const res = withRetryAfter(new Response(null, { status: 429 }), 1500);
		expect(res.headers.get("retry-after")).toBe("2");
	});

	it("leaves the header off when the upstream named no delay", () => {
		const res = withRetryAfter(new Response(null, { status: 429 }), undefined);
		expect(res.headers.get("retry-after")).toBeNull();
	});
});

describe("anthropic-messages encodeStream error frames", () => {
	it("labels a mid-stream 429 as rate_limit_error, not api_error", async () => {
		const sse = encodeStream(
			streamOf([
				{ type: "start", partial: message() },
				{ type: "error", reason: "error", error: rateLimited() },
			]),
			"claude-test",
		);
		const text = await new Response(sse).text();
		expect(text).toContain('"type":"rate_limit_error"');
		expect(text).not.toContain('"type":"api_error"');
	});
});

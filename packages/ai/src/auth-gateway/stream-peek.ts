import { extractRetryHint } from "@oh-my-pi/pi-utils";
import { classifyGatewayError, type GatewayErrorClassification } from "../error/gateway";
import type { AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream } from "../types";
import { AssistantMessageEventStream as EventStreamImpl } from "../utils/event-stream";
import { getHeadersFromError, getRetryAfterMsFromHeaders } from "../utils/retry-after";

/**
 * How long a streamed request waits for the upstream's first real event before
 * the gateway commits to a `200 text/event-stream` response. Upstream
 * rejections (429, 401, 400, 529) arrive well inside this window; a model that
 * is merely slow to its first token just gets its headers a little later.
 */
export const STREAM_PEEK_TIMEOUT_MS = 15_000;

export type PeekedStream =
	| { kind: "error"; classified: GatewayErrorClassification; retryAfterMs?: number }
	| { kind: "stream"; events: AssistantMessageEventStream };

/** Classify a failed assistant message the same way the non-streaming path does. */
export function classifyFailedMessage(message: AssistantMessage): GatewayErrorClassification {
	const text = message.errorMessage ?? "Upstream request failed";
	const classificationText = message.errorClassificationMessage ?? text;
	const classified =
		typeof message.errorStatus === "number"
			? classifyGatewayError(Object.assign(new Error(classificationText), { status: message.errorStatus }))
			: classifyGatewayError(classificationText);
	return { ...classified, message: text };
}

const RETRY_AFTER_MS_HINT = /\bretry-after-ms=(\d+)\b/;

/** Retry delay the upstream stated, from the gateway's `retry-after-ms=` hint or the message text. */
export function retryAfterMsFromMessage(message: AssistantMessage): number | undefined {
	const text = message.errorMessage ?? "";
	const hinted = text.match(RETRY_AFTER_MS_HINT);
	if (hinted) return Number.parseInt(hinted[1]!, 10);
	const extracted = extractRetryHint(undefined, text);
	return typeof extracted === "number" && Number.isFinite(extracted) && extracted > 0 ? extracted : undefined;
}

/** Sets `retry-after` (whole seconds, rounded up) on an error response when the upstream named a delay. */
export function withRetryAfter(response: Response, retryAfterMs: number | undefined): Response {
	if (retryAfterMs !== undefined && retryAfterMs > 0) {
		response.headers.set("retry-after", String(Math.max(1, Math.ceil(retryAfterMs / 1000))));
	}
	return response;
}

/**
 * Wait for the first event after `start`. If the upstream failed before any
 * content, report the failure so the gateway can answer with a real HTTP
 * status (429 + `rate_limit_error` + `retry-after`) instead of a 200 stream
 * whose only frame is an error. Otherwise hand back a stream that replays the
 * buffered events and forwards the rest untouched.
 */
export async function peekStream(
	events: AssistantMessageEventStream,
	signal?: AbortSignal,
	timeoutMs = STREAM_PEEK_TIMEOUT_MS,
): Promise<PeekedStream> {
	const iterator = events[Symbol.asyncIterator]();
	const buffered: AssistantMessageEvent[] = [];
	let finished = false;
	const deadline = Date.now() + timeoutMs;
	let pending: Promise<IteratorResult<AssistantMessageEvent>> | undefined;

	while (true) {
		if (signal?.aborted) break;
		const remaining = deadline - Date.now();
		if (remaining <= 0) break;
		pending ??= iterator.next();
		let timer: NodeJS.Timeout | undefined;
		const timeout = new Promise<"timeout">(resolve => {
			timer = setTimeout(() => resolve("timeout"), remaining);
		});
		let next: IteratorResult<AssistantMessageEvent> | "timeout";
		try {
			next = await Promise.race([pending, timeout]);
		} catch (error) {
			// The provider threw instead of pushing an error event.
			clearTimeout(timer);
			if (buffered.every(e => e.type === "start")) {
				return {
					kind: "error",
					classified: classifyGatewayError(error),
					retryAfterMs: getRetryAfterMsFromHeaders(getHeadersFromError(error)),
				};
			}
			const replay = new EventStreamImpl();
			for (const event of buffered) replay.push(event);
			replay.fail(error);
			return { kind: "stream", events: replay };
		}
		clearTimeout(timer);
		if (next === "timeout") break;
		pending = undefined;
		if (next.done) {
			finished = true;
			break;
		}
		const event = next.value;
		if (event.type === "error" && event.error.stopReason === "error" && buffered.every(e => e.type === "start")) {
			const message = event.error;
			return {
				kind: "error",
				classified: classifyFailedMessage(message),
				retryAfterMs: retryAfterMsFromMessage(message),
			};
		}
		buffered.push(event);
		if (event.type !== "start") break;
	}

	const replay = new EventStreamImpl();
	for (const event of buffered) replay.push(event);
	if (finished) {
		replay.end();
		return { kind: "stream", events: replay };
	}
	void (async () => {
		try {
			if (pending) {
				const next = await pending;
				if (next.done) {
					replay.end();
					return;
				}
				replay.push(next.value);
			}
			while (true) {
				const next = await iterator.next();
				if (next.done) break;
				replay.push(next.value);
			}
			replay.end();
		} catch (error) {
			replay.fail(error);
		}
	})();
	return { kind: "stream", events: replay };
}

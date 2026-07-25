//! Server-Sent Events frame parser (`DESIGN.md` §6.2).
//!
//! Operates on raw bytes so it is safe to feed it directly from `reqwest::Response::bytes_stream()`
//! — a chunk boundary will never split the parser's view of a frame boundary or a multi-byte UTF-8
//! sequence. Complete frames are emitted as soon as their terminating blank line arrives; a trailing
//! frame with no terminator is flushed at EOF.

/// A parsed SSE frame: the joined `data:` payload and an optional named `event:` type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseFrame {
    /// The named event type from an `event:` line, if any.
    pub event: Option<String>,
    /// All `data:` lines joined with `\n`.
    pub data: String,
}

impl SseFrame {
    /// OpenAI's terminal marker, sent as a bare `data: [DONE]` frame.
    pub fn is_done_marker(&self) -> bool {
        self.event.is_none() && self.data == "[DONE]"
    }
}

/// Incremental SSE parser. Feed it byte chunks as they arrive; `finish()` flushes any trailing
/// unterminated frame at EOF.
#[derive(Debug, Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a chunk and return every frame whose terminating blank line is now present.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.buf.extend_from_slice(chunk);
        self.drain_complete()
    }

    /// Flush a trailing frame lacking a terminator (call at EOF). Consumes `self`.
    pub fn finish(mut self) -> Vec<SseFrame> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let trailing = std::mem::take(&mut self.buf);
        parse_frame(&trailing).into_iter().collect()
    }

    fn drain_complete(&mut self) -> Vec<SseFrame> {
        let mut out = Vec::new();
        while let Some((frame_end, delim_len)) = find_boundary(&self.buf) {
            let frame_bytes: Vec<u8> = self.buf.drain(..frame_end + delim_len).collect();
            // frame_bytes[0..frame_end] is the frame; [frame_end..] is the delimiter we drop.
            if let Some(frame) = parse_frame(&frame_bytes[..frame_end]) {
                out.push(frame);
            }
        }
        out
    }
}

/// Parse a complete SSE document into frames (tests / fully-buffered decode).
pub fn parse_sse(input: &str) -> Vec<SseFrame> {
    let mut p = SseParser::new();
    let mut out = p.feed(input.as_bytes());
    out.extend(p.finish());
    out
}

/// Find the earliest frame boundary: a blank line, either `\r\n\r\n` or `\n\n`.
/// Returns `(frame_end, delimiter_length)` where the frame is `buf[..frame_end]`.
fn find_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let rn = memchr_slice(buf, b"\r\n\r\n").map(|p| (p, 4));
    let n = memchr_slice(buf, b"\n\n").map(|p| (p, 2));
    match (rn, n) {
        (None, None) => None,
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
    }
}

/// Parse one frame's bytes into an `SseFrame`, or `None` if it has no `data` and no `event`.
fn parse_frame(frame: &[u8]) -> Option<SseFrame> {
    let mut event: Option<String> = None;
    let mut data_parts: Vec<String> = Vec::new();

    for line in split_lines(frame) {
        // Comment line: starts with ':'.
        if line.starts_with(b":") {
            continue;
        }
        if let Some(rest) = strip_prefix(line, b"data:") {
            data_parts.push(decode(strip_one_leading_space(rest)));
        } else if let Some(rest) = strip_prefix(line, b"event:") {
            event = Some(decode(strip_one_leading_space(rest)));
        } else if strip_prefix(line, b"id:").is_some() || strip_prefix(line, b"retry:").is_some() {
            // Ignored for v1.
        }
        // Empty lines or unknown fields are ignored.
    }

    let data = data_parts.join("\n");
    if event.is_none() && data.is_empty() {
        None
    } else {
        Some(SseFrame { event, data })
    }
}

/// Split frame bytes into lines on `\n`, stripping a trailing `\r` (CRLF tolerance).
fn split_lines(frame: &[u8]) -> impl Iterator<Item = &[u8]> {
    struct Lines<'a> {
        bytes: &'a [u8],
    }
    impl<'a> Iterator for Lines<'a> {
        type Item = &'a [u8];
        fn next(&mut self) -> Option<&'a [u8]> {
            let bytes = std::mem::take(&mut self.bytes);
            if bytes.is_empty() {
                return None;
            }
            let (line, rest) = match memchr_slice(bytes, b"\n") {
                Some(i) => (&bytes[..i], &bytes[i + 1..]),
                None => (bytes, &[][..]),
            };
            self.bytes = rest;
            // Strip a single trailing CR.
            Some(line.strip_suffix(b"\r").unwrap_or(line))
        }
    }
    Lines { bytes: frame }
}

fn strip_prefix<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    line.strip_prefix(prefix)
}

/// Strip exactly one leading ASCII space, matching the SSE spec.
fn strip_one_leading_space(s: &[u8]) -> &[u8] {
    s.strip_prefix(b" ").unwrap_or(s)
}

fn decode(s: &[u8]) -> String {
    String::from_utf8_lossy(s).into_owned()
}

/// A tiny `memchr` over a slice (avoids pulling the `memchr` crate just for this).
fn memchr_slice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(event: Option<&str>, data: &str) -> SseFrame {
        SseFrame {
            event: event.map(str::to_owned),
            data: data.to_owned(),
        }
    }

    #[test]
    fn basic_data_frames_lf() {
        let out = parse_sse("data: hello\n\ndata: world\n\n");
        assert_eq!(out, vec![frame(None, "hello"), frame(None, "world")]);
    }

    #[test]
    fn crlf_boundaries() {
        let out = parse_sse("data: hello\r\n\r\ndata: world\r\n\r\n");
        assert_eq!(out, vec![frame(None, "hello"), frame(None, "world")]);
    }

    #[test]
    fn mixed_lf_and_crlf_boundaries() {
        // First frame terminated by \r\n\r\n, second by \n\n.
        let out = parse_sse("data: a\r\n\r\ndata: b\n\n");
        assert_eq!(out, vec![frame(None, "a"), frame(None, "b")]);
    }

    #[test]
    fn strip_exactly_one_leading_space() {
        assert_eq!(parse_sse("data:no-space\n\n")[0].data, "no-space");
        assert_eq!(parse_sse("data: one-space\n\n")[0].data, "one-space");
        assert_eq!(parse_sse("data:  two-spaces\n\n")[0].data, " two-spaces");
    }

    #[test]
    fn multiple_data_lines_joined_by_newline() {
        let out = parse_sse("data: line1\ndata: line2\n\n");
        assert_eq!(out, vec![frame(None, "line1\nline2")]);
    }

    #[test]
    fn named_event() {
        let out = parse_sse("event: content_block_delta\ndata: {\"x\":1}\n\n");
        assert_eq!(out, vec![frame(Some("content_block_delta"), "{\"x\":1}")]);
    }

    #[test]
    fn comment_lines_ignored() {
        let out = parse_sse(": keepalive\ndata: real\n\n");
        assert_eq!(out, vec![frame(None, "real")]);
    }

    #[test]
    fn id_and_retry_ignored() {
        let out = parse_sse("id: 42\nretry: 5000\ndata: payload\n\n");
        assert_eq!(out, vec![frame(None, "payload")]);
    }

    #[test]
    fn empty_frame_skipped() {
        // No data and no event -> dropped.
        let out = parse_sse("\n\n\n\ndata: keep\n\n");
        assert_eq!(out, vec![frame(None, "keep")]);
    }

    #[test]
    fn trailing_frame_without_terminator_flushed_at_eof() {
        let out = parse_sse("data: done\ndata: trailing");
        assert_eq!(out, vec![frame(None, "done\ntrailing")]);
    }

    #[test]
    fn done_marker_detected() {
        let out = parse_sse("data: [DONE]\n\n");
        assert_eq!(out.len(), 1);
        assert!(out[0].is_done_marker());
    }

    #[test]
    fn incremental_feed_splits_across_chunks() {
        let mut p = SseParser::new();
        assert!(p.feed(b"data: par").is_empty());
        assert!(p.feed(b"t1\n\n").iter().any(|f| f.data == "part1"));
        let rest = p.finish();
        assert!(rest.is_empty());
    }

    #[test]
    fn incremental_feed_byte_split_utf8() {
        // "é" is 0xC3 0xA9. Split it across the data field across two chunks.
        let mut p = SseParser::new();
        assert!(p.feed(b"data: \xC3").is_empty()); // incomplete char, no boundary yet
        let frames = p.feed(b"\xA9\n\n");
        assert_eq!(frames, vec![frame(None, "é")]);
    }
}

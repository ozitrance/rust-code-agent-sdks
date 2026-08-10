//! Raw REST and SSE observation for evidence capture and protocol debugging.
//!
//! [`WireObserver`] is an optional synchronous callback shared by an
//! [`OpencodeClient`](crate::client_async::OpencodeClient) and every event stream
//! it creates. REST request/response bodies are observed as the exact bytes the
//! transport sends or receives. SSE `data` payloads are observed before JSON
//! deserialization, preserving known and unknown event payloads alike.
//!
//! HTTP authentication headers are deliberately not exposed. Observer callbacks
//! should do minimal work (for example, enqueue the observation on a channel)
//! because they run inline with request and stream processing.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// One raw transport observation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WireObservation {
    /// JSON request bytes immediately before the HTTP request is sent.
    HttpRequest {
        /// HTTP method, such as `POST`.
        method: String,
        /// Fully resolved request URL. Authentication headers are not included.
        url: String,
        /// Exact request bytes, or `None` when the request has no body.
        body: Option<Vec<u8>>,
    },
    /// Response bytes immediately after the HTTP response body is read.
    HttpResponse {
        /// HTTP method of the corresponding request.
        method: String,
        /// Fully resolved request URL.
        url: String,
        /// HTTP response status code.
        status: u16,
        /// Exact response body bytes, including an empty successful body.
        body: Vec<u8>,
    },
    /// The SSE connection opened or reopened.
    SseConnected,
    /// One SSE message before typed JSON decoding.
    SseEvent {
        /// SSE event name, empty when the server omitted `event:`.
        event: String,
        /// SSE event id, empty when the server omitted `id:`.
        id: String,
        /// Exact parser-delivered `data` payload.
        data: String,
        /// Server-requested retry duration, if supplied in the frame.
        retry: Option<Duration>,
    },
}

/// Cloneable callback for raw [`WireObservation`] values.
#[derive(Clone)]
pub struct WireObserver {
    callback: Arc<dyn Fn(WireObservation) + Send + Sync + 'static>,
}

impl WireObserver {
    /// Wrap a synchronous observation callback.
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(WireObservation) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub(crate) fn observe(&self, observation: WireObservation) {
        (self.callback)(observation);
    }
}

impl fmt::Debug for WireObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WireObserver(..)")
    }
}

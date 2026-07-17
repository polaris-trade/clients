//! Backend-generic accessors: transports/stats, gap re-request, current-datagram bookkeeping.

use std::sync::Arc;

use transport_core::{AsPayload, DatagramSource, TransportCore};

use super::{MoldUdpReceiver, ReadyItem, ReceiverStats};
use crate::error::MoldUdpError;

impl<T: DatagramSource> MoldUdpReceiver<T> {
    /// Raw access to the bound transports, for callers that need
    /// backend-specific introspection (e.g. the local bound address in tests).
    pub fn transports(&self) -> &[T] {
        &self.transports
    }

    pub fn stats(&self) -> ReceiverStats {
        ReceiverStats {
            arbiter: self.arbiter.as_ref().map(|a| a.stats()),
            pending_gaps: self.gap_handler.pending_gaps(),
        }
    }

    /// Send Request Packets for currently-pending gaps over `transport`
    /// (typically a small unicast socket pointed at `rerequest_server_addr`,
    /// separate from the multicast receive legs). No-op until session id is
    /// known, re-request is enabled, and a rerequest server is configured.
    pub async fn emit_pending_gaps<Tx: TransportCore>(
        &mut self,
        transport: &mut Tx,
    ) -> Result<usize, MoldUdpError> {
        if !self.cfg.rerequest_enabled {
            return Ok(0);
        }
        let Some(emitter) = self.gap_emitter.as_mut() else {
            return Ok(0);
        };
        let Some(&session) = self.session.get() else {
            return Ok(0);
        };
        let gaps = self.gap_handler.pending_gaps();
        if gaps.is_empty() {
            return Ok(0);
        }
        let sent = emitter.emit(&gaps, session, transport).await?;
        if sent > 0 {
            tracing::debug!(sent, "gap re-requests sent");
        }
        Ok(sent)
    }

    /// Resolve an `Inline` item's bytes against whichever of
    /// `current_datagram`/`current_arc` currently backs it.
    pub(super) fn resolve_current(&self, offset: usize, len: usize) -> &[u8] {
        if let Some(arc) = &self.current_arc {
            return &arc.payload()[offset..offset + len];
        }
        let frame = self
            .current_datagram
            .as_ref()
            .expect("Inline ready item pending without a backing datagram");
        &frame.payload()[offset..offset + len]
    }

    /// Release whatever backs an outgoing `Inline` item once no more
    /// outstanding items reference it.
    pub(super) fn retire_current(&mut self, item: &ReadyItem<T::Frame>) {
        if matches!(item, ReadyItem::Inline { .. }) {
            self.current_pending -= 1;
            if self.current_pending == 0 {
                self.current_datagram = None;
                self.current_arc = None;
            }
        }
    }

    /// Promote `current_datagram` to a shared `Arc`, or clone the existing one
    /// if this datagram already needed buffering for an earlier message. At
    /// most one `Arc::new` per datagram.
    pub(super) fn arc_current(&mut self) -> Arc<T::Frame> {
        if let Some(arc) = &self.current_arc {
            return Arc::clone(arc);
        }
        let frame = self
            .current_datagram
            .take()
            .expect("current datagram must be set while its blocks are being processed");
        let arc = Arc::new(frame);
        self.current_arc = Some(Arc::clone(&arc));
        arc
    }
}

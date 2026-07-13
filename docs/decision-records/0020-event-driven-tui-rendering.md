# ADR-0020 — TUI rendering is event-driven, never time-based

**Status**: Accepted

## Context

The TUI originally repainted on a 250 ms interval tick alongside its event
channel. Every state change already arrives as an explicit event — network
lines, key input, RPC outcomes, delivery notifications — so the tick painted
identical frames four times per second for nothing. Two costs made this worse
than idle waste:

- The wrap-aware scroll fix prices each frame at O(buffer): a rendered-row
  count over up to 5,000 buffered lines. A periodic tick turns that into
  continuous background CPU on an idle terminal; event-driven rendering makes
  it a cost paid only when something changed.
- The tick silently masked a missing capability: terminal resize events were
  dropped by the input thread, and the layout only recovered because some
  tick happened to fire soon after. The periodic repaint hid a real gap
  instead of surfacing it.

Rejected alternative: caching rendered-row counts to make ticking cheap —
more machinery to keep an unnecessary timer affordable.

## Decision

Every repaint is caused by an event. The render loop redraws only when a
`Ui` message arrives; there is no display timer of any kind.

- Terminal resize is forwarded from the input thread as an explicit redraw
  request — capabilities are handled, not papered over by polling.
- Any state source added to the TUI must deliver its changes through the
  `Ui` channel, or they will not render. A stale display is diagnosed as a
  missing event, never fixed by reintroducing a tick.
- A future feature that legitimately needs time-based display (a clock, a
  spinner, a countdown) introduces its own purpose-specific event source,
  scoped to that feature and active only while it is visible — and records
  the justification by superseding this ADR. A global interval tick is not
  an acceptable implementation shortcut for it.

## Consequences

- An idle TUI performs zero rendering work; the O(buffer) frame cost is paid
  exactly once per actual change.
- Forgotten-event bugs manifest as visibly stale panes rather than being
  silently corrected within 250 ms — easier to notice, attribute, and fix at
  the source.
- Contributors adding TUI state carry a small obligation (emit an event)
  in exchange for the display layer staying deterministic: frame count is a
  function of events, which also keeps render-path tests meaningful.

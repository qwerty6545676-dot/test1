"""Text formatting for Telegram messages.

All messages go out as ``parse_mode=HTML`` — safer than Markdown
(no accidental ``_`` / ``*`` escapes) and still lets us bold /
monospace important fields. Everything we interpolate is routed
through :func:`html.escape` so a weird symbol or error message can't
break formatting.
"""

from __future__ import annotations

import html

from ..signals import ArbSignal, InfoEvent


_EMOJI_BY_KIND: dict[str, str] = {
    "startup": "🚀",
    "shutdown": "🛑",
    "error": "❌",
    "silence": "🔇",
    "recovery": "✅",
    "reconnect": "🔁",
    "notice": "ℹ️",
    "watchdog": "🐕",
}


def format_arb(sig: ArbSignal) -> str:
    """Render an arb signal as an HTML Telegram message."""
    return (
        f"<b>ARB {sig.market.upper()} {html.escape(sig.symbol)}</b>\n"
        f"buy  <b>{html.escape(sig.buy_ex)}</b> @ <code>{sig.buy_ask:.8f}</code>\n"
        f"sell <b>{html.escape(sig.sell_ex)}</b> @ <code>{sig.sell_bid:.8f}</code>\n"
        f"net  <b>{sig.net_pct:.3f}%</b>"
    )


def format_info(event: InfoEvent) -> str:
    """Render an info event as an HTML Telegram message."""
    emoji = _EMOJI_BY_KIND.get(event.kind, "•")
    market_tag = f" [{event.market}]" if event.market else ""
    return (
        f"{emoji} <b>{event.kind.upper()}</b>{market_tag}\n"
        f"{html.escape(event.message)}"
    )

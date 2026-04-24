"""Telegram output package.

Kept under the name ``telegram_notify`` so the import doesn't shadow
the PyPI ``telegram`` library if anyone pulls it in later.
"""

from .client import TelegramClient
from .notifier import TelegramNotifier

__all__ = ["TelegramClient", "TelegramNotifier"]

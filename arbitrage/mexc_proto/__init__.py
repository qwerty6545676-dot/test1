"""Generated Python classes for MEXC v3 Protobuf WS messages.

Do NOT edit the ``*_pb2.py`` files by hand. Regenerate with::

    python -m grpc_tools.protoc \
        -I proto/mexc --python_out=arbitrage/mexc_proto proto/mexc/*.proto

The original ``.proto`` sources live under ``proto/mexc/`` and come from
https://github.com/mexcdevelop/websocket-proto.

The generated files use flat ``import PublicDealsV3Api_pb2`` style
(protoc's default for ``--python_out``), so we prepend this directory
to ``sys.path`` once at import time. The alternative — rewriting every
generated import to a relative one — has to be redone on every
regeneration, so the sys.path approach wins on maintenance.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(__file__)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

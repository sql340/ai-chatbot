from __future__ import annotations

import logging

from .api import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = create_app()


__all__ = ["app"]


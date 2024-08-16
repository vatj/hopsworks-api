#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import importlib.util
import logging
from typing import TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from rich.logging import RichHandler


_rich_handler = None


def is_rich_logger_enabled():
    global _rich_handler
    return _rich_handler is not None


def append_rich_handler_to_logger(
    logger: Optional[logging.Logger] = None,
    handler: Optional[RichHandler] = None,
    remove_existing_handlers: bool = False,
):
    global _rich_handler
    if importlib.util.find_spec("rich") is not None:
        from rich.logging import RichHandler
    else:
        raise ImportError(
            "rich package is not installed, please install it to use rich logger"
        )

    if handler is None:
        rich_handler = RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
        )
    else:
        rich_handler = handler

    if logger is None:
        logger = logging.getLogger("hopsworks")
        _rich_handler = rich_handler

    if remove_existing_handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logger.addHandler(rich_handler)
    return logger

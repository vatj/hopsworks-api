#
#   Copyright 2024 HOPSWORKS AB
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

import os

from hopsworks_common.helpers import verbose_constants
from rich.console import Console


_rich_console = None


def enable_rich_verbose_mode() -> None:
    os.environ[verbose_constants.VERBOSE_ENV_VAR] = "true"
    os.environ[verbose_constants.USE_RICH_CONSOLE_ENV_VAR] = "true"


def is_rich_print_enabled() -> bool:
    use_rich = os.getenv(verbose_constants.USE_RICH_CONSOLE_ENV_VAR, "true").lower()
    return use_rich == "true" or use_rich == "1"


def is_hopsworks_verbose() -> bool:
    hopsworks_verbose = os.getenv(verbose_constants.VERBOSE_ENV_VAR, "1").lower()
    return hopsworks_verbose == "true" or hopsworks_verbose == "1"


def init_rich_with_default_config() -> None:
    global _rich_console
    if _rich_console is None:
        _rich_console = Console(**verbose_constants.DEFAULT_VERBOSE_CONFIG)


def get_rich_console() -> Console:
    global _rich_console
    if _rich_console is None:
        init_rich_with_default_config()
    return _rich_console


def get_python_lexer_theme() -> str:
    return verbose_constants.PYTHON_LEXER_THEME


def start_rich_recording() -> None:
    global _rich_console
    if _rich_console is None:
        enable_rich_verbose_mode()
        init_rich_with_default_config()
    _rich_console.record = True
    _rich_console.log("[bold]Recording script execution using rich...[/bold]")


def is_rich_recording() -> bool:
    global _rich_console
    return _rich_console.record


def stop_rich_recording() -> None:
    global _rich_console
    _rich_console.log("[bold]Stop recording script execution...[/bold]")

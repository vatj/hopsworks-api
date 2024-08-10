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

from typing import TYPE_CHECKING, Any, Dict, Optional

from hopsworks_common.core import constants
from hopsworks_common.helpers import verbose


if TYPE_CHECKING:
    pass

if constants.HAS_RICH:
    from hopsworks_common.helpers import rich_messages
    from rich import box
    from rich.markdown import Markdown
    from rich.panel import Panel
    from rich.table import Column, Table


def print_connected_to_hopsworks_message(project_name: str, hostname: str):
    if verbose.is_hopsworks_verbose() and verbose.is_rich_print_enabled():
        rich_console = verbose.get_rich_console()
        rich_console.print(
            Panel.fit(
                rich_messages.CONNECTED_TO_PROJECT.format(
                    project_name=project_name, hostname=hostname
                ),
                title="Connected to Hopsworks!",
                style="bold",
                box=box.ASCII2,
                padding=(1, 2),
            ),
            justify="center",
        )
    else:
        print(f"Connected to Hopsworks project {project_name} on {hostname}.")


def print_connected_to_feature_store_message(summary: Optional[Dict[str, Any]] = None):
    if verbose.is_hopsworks_verbose() and verbose.is_rich_print_enabled():
        summary_table = Table(
            Column("Feature Groups", justify="center"),
            Column("Feature Views", justify="center"),
            Column("Training Datasets", justify="center"),
            Column("Storage Connectors", justify="center"),
            title=Markdown("## Feature Store Summary"),
            show_header=True,
            show_footer=False,
            header_style="bold",
            box=box.ASCII2,
            show_lines=True,
        )
        summary_table.add_row(
            *[
                str(summary.get(key, 0))
                for key in [
                    "num_feature_groups",
                    "num_feature_views",
                    "num_training_datasets",
                    "num_storage_connectors",
                ]
            ],
        )
        method_message = Markdown(
            ("## Methods\n" if summary else "")
            + rich_messages.HAS_GETTING_STARTED_METHOD.format(
                class_name="Feature Store"
            )
            + rich_messages.FEATURE_STORE_SHOW_FEATURE_GROUPS
            + rich_messages.FEATURE_STORE_SHOW_FEATURE_VIEWS,
            justify="left",
            inline_code_lexer="python",
            inline_code_theme=verbose.get_python_lexer_theme(),
        )
        rich_console = verbose.get_rich_console()
        (
            rich_console.print(
                summary_table,
                method_message,
                justify="center",
            ),
        )
    else:
        the_string = "Hopsworks Feature Store: " + "\n\t- ".join(
            [
                f"Feature Groups : {summary.get('num_feature_groups', 0)}",
                f"Feature Views : {summary.get('num_feature_views', 0)}",
                f"Training Datasets : {summary.get('num_training_datasets', 0)}",
                f"Storage Connectors : {summary.get('num_storage_connectors', 0)}",
            ]
        )
        print(the_string)

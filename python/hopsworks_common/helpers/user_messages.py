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

from typing import TYPE_CHECKING

from hopsworks_common import client
from hopsworks_common.helpers import verbose
from rich import box
from rich.markdown import Markdown
from rich.panel import Panel


if TYPE_CHECKING:
    from hsfs import feature_store as fs_mod


def print_connected_to_hopsworks_message(project_name: str, hostname: str):
    if verbose.is_hopsworks_verbose() and verbose.is_rich_print_enabled():
        rich_console = verbose.get_rich_console()
        rich_console.print(
            Panel.fit(
                f"Connected to Project [bold red]{project_name}[/bold red] on [italic red]{hostname}[/italic red].",
                title="Hopsworks",
                style="bold",
                box=box.ASCII2,
                padding=(1, 2),
            ),
        )
    else:
        print(f"Connected to project {project_name} in Hopsworks.")


def print_connected_to_feature_store_message(fs_obj: fs_mod.FeatureStore):
    get_started_message = Markdown(
        "- Call the `getting_started()` method to get an overview of the feature store API and capabilities.\n"
        "- Call `show_feature_groups()` to show a list of existing Feature Groups to insert/upsert new data or "
        "set `with_features=True` to see which features you can select to build a new Feature View.\n"
        "- Call `show_feature_views()` to show a list of existing Feature Views, you can use them to read data "
        "and create Training Datasets. Feature Views composed of Features from online-enabled FeatureGroups can "
        "be used to serve feature value for real-time use cases. Checkout the ⚡ "
        "[benchmarks](https://www.hopsworks.ai/post/feature-store-benchmark-comparison-hopsworks-and-feast)",
        justify="left",
        inline_code_lexer="python",
        inline_code_theme=verbose.get_python_lexer_theme(),
    )

    if verbose.is_hopsworks_verbose() and verbose.is_rich_print_enabled():
        rich_console = verbose.get_rich_console()
        (
            rich_console.print(
                Panel.fit(
                    f"Connected to Project [bold red]{fs_obj.project_name}[/bold red] on [italic red]{client.get_instance()._host}[/italic red].",
                    title="Hopsworks Feature Store",
                    style="bold",
                    box=box.ASCII2,
                    padding=(1, 2),
                ),
                get_started_message,
                justify="center",
            ),
        )
    else:
        print(f"Connected to project {fs_obj.project_name} in Hopsworks Feature Store.")

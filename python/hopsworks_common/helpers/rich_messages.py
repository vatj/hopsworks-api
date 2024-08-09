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

## General messages
CONNECTED_TO_PROJECT = "Connected to Project [bold red]{project_name}[/bold red] on [italic red]{hostname}[/italic red]."
HAS_GETTING_STARTED_METHOD = "- Call `getting_started()` method to get an overview of the {class_name} API and capabilities.\n"

## Feature Store messages
FEATURE_STORE_SHOW_FEATURE_GROUPS = (
    "- Call `show_feature_groups()` method to show a list of existing Feature Groups "
    "to insert/upsert new data or set `with_features=True` "
    "to see which features you can select to build a new Feature View.\n"
)
FEATURE_STORE_SHOW_FEATURE_VIEWS = (
    "- Call `show_feature_views()` to show a list of existing Feature Views, you can use them to read data "
    "and create Training Datasets. Feature Views composed of Features from online-enabled FeatureGroups can "
    "be used to serve feature value for real-time use cases. Checkout our "
    "[benchmarks](https://www.hopsworks.ai/post/feature-store-benchmark-comparison-hopsworks-and-feast) ⚡"
)

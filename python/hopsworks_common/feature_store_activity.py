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

import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import humps
from hopsworks import execution as execution_mod
from hopsworks_common import user as user_mod
from hopsworks_common import util
from hopsworks_common.core.constants import HAS_GREAT_EXPECTATIONS, HAS_RICH
from hsfs import expectation_suite as es_mod
from hsfs import validation_report as vr_mod


if TYPE_CHECKING or HAS_GREAT_EXPECTATIONS:
    from great_expectations.core import (
        ExpectationSuite,
        ExpectationSuiteValidationResult,
    )

if HAS_RICH:
    from rich import box
    from rich.markdown import Markdown
    from rich.table import Table

    ACTIVITY_STATISTICS_MARKDOWN = (
        """Percentage of data: {row_percentage}%,\n{window_start} to {window_end}."""
    )
    COMMIT_STATISTICS_MARKDOWN = """Ingested {rows_ingested} rows:\n- inserted: {percentage_rows_inserted:.0f}%\n- updated: {percentage_rows_updated:.0f}%\n- deleted: {percentage_rows_deleted:.0f}%"""
    EMPTY_COMMIT_MARKDOWN = "Ingestion Job was triggered but found no ingested data."


class FeatureStoreActivityType(Enum):
    METADATA = "METADATA"
    STATISTICS = "STATISTICS"
    JOB = "JOB"
    VALIDATIONS = "VALIDATIONS"
    EXPECTATIONS = "EXPECTATIONS"
    COMMIT = "COMMIT"


@dataclass(init=False, repr=False)
class FeatureStoreActivity:
    type: FeatureStoreActivityType
    timestamp: int
    metadata: str
    user: Optional[user_mod.User]
    # optional fields depending on the activity type
    validation_report: Optional[
        Union[vr_mod.ValidationReport, ExpectationSuiteValidationResult]
    ] = None
    expectation_suite: Optional[Union[es_mod.ExpectationSuite, ExpectationSuite]] = None
    commit: Optional[Dict[str, Union[str, int, float]]] = None
    statistics: Optional[Dict[str, Union[str, int, float]]] = None
    execution: Optional[execution_mod.Execution] = None
    execution_last_event_time: Optional[int] = None
    # internal fields
    id: int
    href: str

    def __init__(
        self,
        type: str,
        timestamp: int,
        metadata: Optional[str] = None,
        user: Optional[Dict[str, Any]] = None,
        expectation_suite: Optional[Dict[str, Any]] = None,
        validation_report: Optional[Dict[str, Any]] = None,
        commit: Optional[Dict[str, Union[str, int, float]]] = None,
        statistics: Optional[Dict[str, Union[str, int, float]]] = None,
        execution: Optional[Dict[str, Any]] = None,
        execution_last_event_time: Optional[int] = None,
        **kwargs,
    ):
        self.type = FeatureStoreActivityType(type) if isinstance(type, str) else type
        self.timestamp = timestamp

        self.id = kwargs.get("id")
        self.href = kwargs.get("href")

        self.user = user_mod.User.from_response_json(user) if user else None
        self.metadata = metadata
        self.commit = commit
        self.statistics = statistics
        self.execution = (
            execution_mod.Execution.from_response_json(execution) if execution else None
        )
        self.execution_last_event_time = execution_last_event_time

        if self.type == FeatureStoreActivityType.VALIDATIONS and validation_report:
            self.validation_report = vr_mod.ValidationReport.from_response_json(
                validation_report
            )
            if HAS_GREAT_EXPECTATIONS:
                self.validation_report = self.validation_report.to_ge_type()

        if self.type == FeatureStoreActivityType.EXPECTATIONS and expectation_suite:
            self.expectation_suite = es_mod.ExpectationSuite.from_response_json(
                expectation_suite
            )
            if HAS_GREAT_EXPECTATIONS:
                self.expectation_suite = self.expectation_suite.to_ge_type()

    @classmethod
    def from_response_json(
        cls, response_json: Dict[str, Any]
    ) -> List[FeatureStoreActivity]:
        if "items" in response_json:
            return [
                cls.from_response_json(activity) for activity in response_json["items"]
            ]
        else:
            return cls(**humps.decamelize(response_json))

    def to_dict(self) -> Dict[str, Any]:
        activity_dict = {
            "id": self.id,
            "type": self.type.value,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
        }
        if self.user:
            activity_dict["user"] = self.user.to_dict()
        if self.validation_report:
            activity_dict["validation_report"] = (
                self.validation_report.to_dict()
                if hasattr(self.validation_report, "_id")
                else self.validation_report.to_json_dict()
            )
        if self.expectation_suite:
            activity_dict["expectation_suite"] = (
                self.expectation_suite.to_dict()
                if hasattr(self.expectation_suite, "_id")
                else self.expectation_suite.to_json_dict()
            )
        if self.commit:
            activity_dict["commit"] = self.commit
        if self.statistics:
            activity_dict["statistics"] = self.statistics
        if self.execution:
            activity_dict["execution"] = humps.decamelize(
                json.loads(self.execution.json())
            )

        return activity_dict

    def json(self) -> str:
        return json.dumps(self.to_dict(), cls=util.Encoder)

    def __repr__(self):
        utc_human_readable = datetime.datetime.fromtimestamp(
            self.timestamp / 1000, datetime.timezone.utc
        ).strftime(r"%Y-%m-%d %H:%M:%S UTC")
        the_string = f"Activity {self.type.value},"
        the_string += f" at: {utc_human_readable}"
        if self.user:
            the_string += f", by: {self.user.email}"
        if self.metadata:
            the_string += f"\n\t{self.metadata}"
        if self.execution:
            if self.execution.execution_final_status:
                the_string += f"\n{self.execution.final_status} at {self.execution_last_event_time}"
            the_string += (
                f"\nCheck it in the Hopsworks UI : {self.execution.get_url()},"
            )
        if self.validation_report:
            the_string += f"Validation {'succeeded' if self.validation_report.success else 'failed'}."
        if self.expectation_suite:
            the_string += (
                f"It has {len(self.expectation_suite.expectations)} expectations."
            )
        if self.statistics:
            the_string += f"\nComputed following statistics:\n{json.dumps(self.statistics, indent=2)}"
        if self.commit:
            the_string += f"\nData ingestion:\n{json.dumps(self.commit, indent=2)}"

        return the_string

    def _to_rich_row(self) -> List[Any]:
        row_entries = [
            self.type.value,
        ]
        row_entries.append(
            datetime.datetime.fromtimestamp(
                self.timestamp / 1000, datetime.timezone.utc
            ).strftime(r"%Y-%m-%d %H:%M:%SUTC")
        )
        if self.type == FeatureStoreActivityType.METADATA:
            row_entries.append(self.metadata)
        if self.execution:
            row_entries.append(
                Markdown(f"[Check it here ]({self.execution.get_url()})")
            )
        if self.validation_report:
            row_entries.append(
                f"Validation {'Succeeded' if self.validation_report.success else 'Failed'}"
            )
        if self.expectation_suite:
            row_entries.append(
                f"Updated : {len(self.expectation_suite.expectations)} expectations"
            )
        if self.statistics:
            row_entries.append(build_statistics_markdown(self.statistics))
        if self.commit:
            row_entries.append(build_commit_markdown(self.commit))

        row_entries.append(self.user.email if self.user else "")
        return row_entries


if HAS_RICH:

    def build_activity_table(activities: List[FeatureStoreActivity]):
        activities = sorted(activities, key=lambda x: x.timestamp, reverse=True)
        table = Table(
            show_header=True,
            header_style="bold",
            box=box.ASCII2,
            title="Activity Timeline",
        )
        table.add_column("Type")
        table.add_column("Timestamp")
        table.add_column("Metadata")
        table.add_column("User")

        for activity in activities:
            table.add_row(*activity._to_rich_row())

        return table

    def build_statistics_markdown(
        statistics: Dict[str, Union[str, int, float]],
    ) -> Markdown:
        window_start = statistics.get("window_start_commit_time", 0)
        if window_start == 0:
            window_start_str = "ingested up"
        else:
            window_start_str = datetime.datetime.fromtimestamp(
                window_start / 1000,
                datetime.timezone.utc,
            ).strftime(r"%Y-%m-%d %H:%M:%SUTC")
        return Markdown(
            ACTIVITY_STATISTICS_MARKDOWN.format(
                window_start=window_start_str,
                window_end=datetime.datetime.fromtimestamp(
                    statistics.get("window_end_commit_time") / 1000,
                    datetime.timezone.utc,
                ).strftime(r"%Y-%m-%d %H:%M:%SUTC"),
                row_percentage=statistics.get("row_percentage", 0) * 100,
            )
        )

    def build_commit_markdown(
        commit: Dict[str, Union[str, int, float]],
    ) -> Markdown:
        rows_ingested = (
            commit.get("rows_inserted", 0)
            + commit.get("rows_updated", 0)
            + commit.get("rows_deleted", 0)
        )
        if rows_ingested == 0:
            return Markdown(EMPTY_COMMIT_MARKDOWN)
        return Markdown(
            COMMIT_STATISTICS_MARKDOWN.format(
                rows_ingested=rows_ingested,
                percentage_rows_inserted=commit.get("rows_inserted", 0)
                / rows_ingested
                * 100,
                percentage_rows_updated=commit.get("rows_updated", 0)
                / rows_ingested
                * 100,
                percentage_rows_deleted=commit.get("rows_deleted", 0)
                / rows_ingested
                * 100,
            )
        )

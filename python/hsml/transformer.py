#
#   Copyright 2022 Logical Clocks AB
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

from typing import Optional, Union

import humps
from hopsworks_common import client, util
from hopsworks_common.constants import RESOURCES, Default
from hsml.deployable_component import DeployableComponent
from hsml.resources import TransformerResources


class Transformer(DeployableComponent):
    """Metadata object representing a transformer to be used in a predictor."""

    def __init__(
        self,
        script_file: str,
        resources: Optional[Union[TransformerResources, dict, Default]] = None,  # base
        **kwargs,
    ):
        resources = (
            self._validate_resources(
                util.get_obj_from_json(resources, TransformerResources)
            )
            or self._get_default_resources()
        )
        if resources.num_instances is None:
            resources.num_instances = self._get_default_num_instances()

        super().__init__(script_file, resources)

    def describe(self):
        """Print a description of the transformer"""
        util.pretty_print(self)

    @classmethod
    def _validate_resources(cls, resources):
        if resources is not None:
            # ensure scale-to-zero for kserve deployments when required
            if resources.num_instances != 0 and client.is_scale_to_zero_required():
                raise ValueError(
                    "Scale-to-zero is required for KServe deployments in this cluster. Please, set the number of transformer instances to 0."
                )
        return resources

    @classmethod
    def _get_default_num_instances(cls):
        return (
            0  # enable scale-to-zero by default if required
            if client.is_scale_to_zero_required()
            else RESOURCES.MIN_NUM_INSTANCES
        )

    @classmethod
    def _get_default_resources(cls):
        return TransformerResources(cls._get_default_num_instances())

    @classmethod
    def from_json(cls, json_decamelized):
        sf, rc = cls.extract_fields_from_json(json_decamelized)
        return Transformer(sf, rc) if sf is not None else None

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        sf = util.extract_field_from_json(
            json_decamelized, ["transformer", "script_file"]
        )
        rc = TransformerResources.from_json(json_decamelized)
        return sf, rc

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(*self.extract_fields_from_json(json_decamelized))
        return self

    def to_dict(self):
        return {"transformer": self._script_file, **self._resources.to_dict()}

    def __repr__(self):
        return f"Transformer({self._script_file!r})"

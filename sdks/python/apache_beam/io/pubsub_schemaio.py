#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.coders import RowCoder
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.typehints.schemas import named_tuple_to_schema

__all__ = [
    'WriteToPubsub',
    'ReadFromPubsub',
]

def default_io_expansion_service():
    return BeamJarExpansionService(
        ':sdks:java:extensions:schemaio-expansion-service:shadowJar')

ReadFromWriteToPubsubSchema = typing.NamedTuple(
    'ReadFromWriteToPubsubSchema',
    [
        ('location', unicode),
        ('config', bytes),
        ('data_schema', bytes),
    ],
)

Config = typing.NamedTuple(
    'Config',
    [
        ('timestamp_attribute_key', typing.Optional[unicode]),
        ('dead_letter_queue', typing.Optional[unicode]),
    ],
)

class WriteToPubsub(ExternalTransform):
    URN = "beam:external:java:schemaio:pubsub:write:v1"

    def __init__(
            self,
            location,
            data_schema,
            timestamp_attribute_key=None,
            dead_letter_queue=None,
            expansion_service=None
    ):
        super(WriteToPubsub, self).__init__(
            self.URN,
            NamedTupleBasedPayloadBuilder(
                ReadFromWriteToPubsubSchema(
                    location=location,
                    data_schema=named_tuple_to_schema(data_schema).SerializeToString(),
                    config=RowCoder(typing_to_runner_api(Config).row_type.schema).encode(
                        Config(
                            timestamp_attribute_key=timestamp_attribute_key,
                            dead_letter_queue=dead_letter_queue,
                        )
                    )
                )
            ),
            expansion_service or default_io_expansion_service(),
        )

class ReadFromPubsub(ExternalTransform):
    URN = "beam:external:java:schemaio:pubsub:read:v1"

    def __init__(
            self,
            location,
            data_schema,
            timestamp_attribute_key=None,
            dead_letter_queue=None,
            expansion_service=None
    ):
        super(ReadFromPubsub, self).__init__(
            self.URN,
            NamedTupleBasedPayloadBuilder(
                ReadFromWriteToPubsubSchema(
                    location=location,
                    data_schema=named_tuple_to_schema(data_schema).SerializeToString(),
                    config=RowCoder(typing_to_runner_api(Config).row_type.schema).encode(
                        Config(
                            timestamp_attribute_key=timestamp_attribute_key,
                            dead_letter_queue=dead_letter_queue,
                        )
                    )
                )
            ),
            expansion_service or default_io_expansion_service(),
        )
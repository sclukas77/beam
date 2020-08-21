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

# pytype: skip-file

##must connect to pubsub topic, not subscription

##import pubsub_matcher to compare expected and actual output

from __future__ import absolute_import
from __future__ import division

import json
import logging
import typing
import math
import os
import tempfile
import unittest
from builtins import range
from typing import List
import sys

from past.builtins import unicode

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import hamcrest as hc

import avro
import avro.datafile
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from fastavro.schema import parse_schema
from fastavro import writer

#try:
from avro.schema import Parse  # avro-python3 library for python3
#except ImportError:
#from avro.schema import parse as Parse  # avro library for python2
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

import apache_beam as beam
import tempfile
from apache_beam import Create
from apache_beam.io import avroio
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io import source_test_utils
from apache_beam.io.avroio import _create_avro_sink  # For testing
from apache_beam.io.avroio import _create_avro_source  # For testing
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam import coders
from fastavro import writer

from apache_beam.io.avro_schemaio import WriteToAvro
from apache_beam.io.avro_schemaio import ReadFromAvro

from fastavro.schema import parse_schema

# Import snappy optionally; some tests will be skipped when import fails.
try:
  import snappy  # pylint: disable=import-error
except ImportError:
  snappy = None  # pylint: disable=invalid-name
  logging.warning('python-snappy is not installed; some tests will be skipped.')

AvroWriteTestRow = typing.NamedTuple(
    "AvroWriteTestRow",
    [
        ("name", unicode),
        ("favorite_number", int),
        ("favorite_color", unicode),
    ],
)

RECORDS = [
    AvroWriteTestRow("Thomas", 1, "blue"),
    AvroWriteTestRow("Henry", 3, "green")
]

RECORDS_DICT = [{
    'name': 'Thomas', 'favorite_number': 1, 'favorite_color': 'blue'
}, {
    'name': 'Henry', 'favorite_number': 3, 'favorite_color': 'green'
}]

SCHEMA_STRING = '''
{"namespace": "example.avro",
"type": "record",
"name": "User",
"fields": [
   {"name": "name", "type": "string"},
   {"name": "favorite_number",  "type": ["int", "null"]},
   {"name": "favorite_color", "type": ["string", "null"]}
]
}
'''

SCHEMA = parse_schema(json.loads(SCHEMA_STRING))
_temp_files = []

coders.registry.register_coder(AvroWriteTestRow, coders.RowCoder)

class CrossLanguageAvroIOTest(unittest.TestCase):
  def test_xlang_avro_write(self):
    file_name = 'test_avro_write'
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
              p
              | beam.Create(RECORDS).with_output_types(AvroWriteTestRow)
              | 'Write to jdbc' >> WriteToAvro(file_name, AvroWriteTestRow)
      )

    with TestPipeline() as p:
      readback_data = (
        p
        | Create([file_name])
        | avroio.ReadAllFromAvro()
      )
    assert_that(readback_data,equal_to(RECORDS))

  def test_xlang_avro_read(self):
      path = _write_data()
      #with TestPipeline() as p:
       #   _ = (
        #          p
         #         | beam.Create(RECORDS).with_output_types(AvroWriteTestRow)
          #        | avroio.WriteToAvro(file_name, Parse(SCHEMA_STRING))
          #)

      with TestPipeline() as p:
          p.not_use_test_runner_api = True
          result = (
              p
          #| Create([path])
              | 'Read from jdbc' >> ReadFromAvro(path, AvroWriteTestRow)
          )

      assert_that(result, equal_to(RECORDS))

def _write_data(
      directory=None,
      prefix=tempfile.template,
      codec='null',
      count=len(RECORDS_DICT),
      **kwargs):
    all_records = RECORDS_DICT * \
      (count // len(RECORDS_DICT)) + RECORDS_DICT[:(count % len(RECORDS_DICT))]
    with tempfile.NamedTemporaryFile(delete=False,
                                     dir=directory,
                                     prefix=prefix,
                                     mode='w+b') as f:
      writer(f, SCHEMA, all_records, codec=codec, **kwargs)
      _temp_files.append(f.name)
    return f.name






if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()






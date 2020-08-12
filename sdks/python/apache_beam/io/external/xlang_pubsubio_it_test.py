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

import logging
import unittest
import uuid
import typing
import argparse
import os

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp import pubsub_it_pipeline
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

from apache_beam.io.pubsub_schemaio import ReadFromPubsub
from apache_beam.io.pubsub_schemaio import WriteToPubsub
from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.coders import RowCoder
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.typehints.schemas import named_tuple_to_schema

INPUT_TOPIC = 'psit_topic_input'
OUTPUT_TOPIC = 'psit_topic_output'
INPUT_SUB = 'psit_subscription_input'
OUTPUT_SUB = 'psit_subscription_output'

# How long TestXXXRunner will wait for pubsub_it_pipeline to run before
# cancelling it.
TEST_PIPELINE_DURATION_MS = 3 * 60 * 1000
# How long PubSubMessageMatcher will wait for the correct set of messages to
# appear.
MESSAGE_MATCHER_TIMEOUT_S = 5 * 60

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/local/google/home/slukas/Documents/auth.json"

data_schema_test = typing.NamedTuple(
    "data_schema_test",
    [
        ("event_timestamp", unicode),
        ("f_string", unicode),
        ("f_id", int),
    ]
)

class PubSubSchemaIOIntegrationTest(unittest.TestCase):
    ID_LABEL = 'id'
    TIMESTAMP_ATTRIBUTE = 'timestamp'
    INPUT_MESSAGES = [
        PubsubMessage(b'data001', {}),
        PubsubMessage(b'data002', {
                  TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
              }),
        PubsubMessage(b'data003\xab\xac', {}),
        PubsubMessage(
            b'data004\xab\xac', {
                TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
            })
    ]

    EXPECTED_OUTPUT_MESSAGES = [
          PubsubMessage(b'data001-seen', {'processed': 'IT'}),
          PubsubMessage(
              b'data002-seen',
              {
                  TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
                  TIMESTAMP_ATTRIBUTE + '_out': '2018-07-11T02:02:50.149000Z',
                  'processed': 'IT',
              }),
          PubsubMessage(b'data003\xab\xac-seen', {'processed': 'IT'}),
          PubsubMessage(
              b'data004\xab\xac-seen',
              {
                  TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
                  TIMESTAMP_ATTRIBUTE + '_out': '2018-07-11T02:02:50.149000Z',
                  'processed': 'IT',
              })
      ]

    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.runner_name = type(self.test_pipeline.runner).__name__
        self.project = self.test_pipeline.get_option('project')
        self.uuid = str(uuid.uuid4())

        #Set up PubSub environment.
        from google.cloud import pubsub
        self.pub_client = pubsub.PublisherClient()
        self.input_topic = self.pub_client.create_topic(
            self.pub_client.topic_path(self.project, INPUT_TOPIC + self.uuid))
        self.output_topic = self.pub_client.create_topic(
            self.pub_client.topic_path(self.project, OUTPUT_TOPIC + self.uuid))

        self.sub_client = pubsub.SubscriberClient()
        self.input_sub = self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project, INPUT_SUB + self.uuid),
            self.input_topic.name)
        self.output_sub = self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project, OUTPUT_SUB + self.uuid),
            self.output_topic.name)

    def tearDown(self):
        test_utils.cleanup_subscriptions(
            self.sub_client, [self.input_sub, self.output_sub])
        test_utils.cleanup_topics(
            self.pub_client, [self.input_topic, self.output_topic])

    #def _test_streaming(self):
    def test_xlang_pubsub_write(self):
        data_schema = named_tuple_to_schema(data_schema_test).SerializeToString()
        print(data_schema)
        print(type(data_schema))
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        #don't need with_attributes
        pubsub_msg_verifier = PubSubMessageMatcher(
            self.project,
            self.output_sub, #####no subscriber in, but maybe subscriber out for matcher? Or recreat matcher logic w topic?
            self.EXPECTED_OUTPUT_MESSAGES,
            timeout=MESSAGE_MATCHER_TIMEOUT_S,
            with_attributes=False,
            strip_attributes=None)

        ##can the topic act like the subscription?

        extra_opts = {
            'input_subscription': self.input_sub.name,
            'output_topic': self.output_topic.name,
            'wait_until_finish_duration': TEST_PIPELINE_DURATION_MS,
            'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
        }

        argv = self.test_pipeline.get_full_options_as_args(**extra_opts)

        for msg in self.INPUT_MESSAGES:
            self.pub_client.publish(self.input_topic.name, msg.data, **msg.attributes)

        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--output_topic',
            required=True,
            help=(
                'Output PubSub topic of the form '
                '"projects/<PROJECT>/topic/<TOPIC>".'))
        parser.add_argument(
            '--input_subscription',
            required=True,
            help=(
                'Input PubSub subscription of the form '
                '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
        known_args, pipeline_args = parser.parse_known_args(argv)

        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options.view_as(StandardOptions).streaming = True
        p = beam.Pipeline(options=pipeline_options)
        runner_name = type(p.runner).__name__
        ####don't use sql queries
        result = (
            p
            | WriteToPubsub(
            location=self.input_topic.name,#####maybe should be input topic?
            data_schema=data_schema_test,
            #timestamp_attribute_key=,
            #dead_letter_queue=
        )
        )

    #@attr('IT')
    #def test_writer(self):
    #    self._test_streaming()


        #pubsub_it_pipeline.run_pipeline(
        #    argv=self.test_pipeline.get_full_options_as_args(**extra_opts),
        ##    with_attributes=False,
         #   id_label=self.ID_LABEL,
          #  timestamp_attribute=self.TIMESTAMP_ATTRIBUTE
        #)
        #argv=self.test_pipeline.get_full_options_as_args(**extra_opts)
        #parser = argparse.ArgumentParser()
        #parser.add_argument(
         #   '--output_topic',
          #  required=True,
           # help=(
            #    'Output PubSub topic of the form '
             #   '"projects/<PROJECT>/topic/<TOPIC>".'))
        #known_args, pipeline_args = parser.parse_known_args(argv)

        #pipeline_options = PipelineOptions(pipeline_args)
        #pipeline_options.view_as(StandardOptions).streaming = True
        #p = beam.Pipeline(options=pipeline_options)

        #_ =


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()




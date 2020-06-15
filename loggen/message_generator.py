# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pushes defined number of random messages to the Pub/Sub."""

import argparse
import datetime
import json
import random
import signal
import sys
import time

from google.cloud import pubsub_v1

# Configure Command line parser for arguments
cmd_flags_parser = argparse.ArgumentParser(
    description='Publish messages to Pub/Sub',
    prefix_chars='-')
cmd_flags_parser.add_argument('--event_count', type=int,
                              help='Number of log events to generate',
                              default=-1)
cmd_flags_parser.add_argument('--topic', type=str,
                              help='Name of the Pub/Sub topic')
cmd_flags_parser.add_argument('--project-id', type=str,
                              help='GCP Project Id running the Pub/Sub')
cmd_flags_parser.add_argument('--enable-log', type=bool,
                              default=False,
                              help='print logs')

# Extract command-line arguments
cmd_arguments = cmd_flags_parser.parse_args()

# Define configuration
_LOGGING_ENABLED = cmd_arguments.enable_log
_EXPERIMENT_VARIANTS = ['default', '1', '2', '3']
_SEND_EVENTS_COUNT = cmd_arguments.event_count  # Default send infinite messages
_PUB_SUB_TOPIC = cmd_arguments.topic
_GCP_PROJECT_ID = cmd_arguments.project_id
_PUBLISHER = pubsub_v1.PublisherClient()
_START_TIME = time.time()
_TOPIC_PATH = _PUBLISHER.topic_path(_GCP_PROJECT_ID, _PUB_SUB_TOPIC)

message_count = 0


def build_user_id():
  """
  Generates random user ids with some overlap to simulate a real world
  user behaviour on an app or website.
  :return: A slowly changing random number.
  """
  elapsed_tens_minutes = int((time.time() - _START_TIME) / 600) + 1
  present_millis = int(1000 * (time.time() - int(time.time())))

  if present_millis == 0:
    present_millis = random.randint(1,1000)

  if _LOGGING_ENABLED:
    print(
      'generating user_id: elapsed_tens_minute = {}, present_millis = {}'.format(
        elapsed_tens_minutes, present_millis))

  return random.randint(elapsed_tens_minutes + present_millis,
                        (10 + elapsed_tens_minutes) * present_millis)


def build_message():
  """ Generates an event message imitation
  :return: A random event message
  """
  return dict(
      uid=build_user_id(),
      # change experiment ids based on date/time
      experiment_id=random.randint(1, 100),
      variant=_EXPERIMENT_VARIANTS[random.randint(0, 3)],
      timestamp=datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))


def send_to_pub_sub(message):
  """ Sends the provided payload as JSON to Pub/Sub.
  :param message: the Event information payload
  :return: the published message future.
  """
  return _PUBLISHER.publish(_TOPIC_PATH,
                            data=json.dumps(message).encode('utf-8'))


def print_message_count_before_exit(sig, frame):
  """ Interrupt handler to print the count of messages sent to pub/sub before
  exiting python.
  :param sig: the interrupt signal.
  :param frame: the stackframe.
  """
  print('\nSent {} messages.\nExiting'.format(message_count))
  sys.exit(0)


# Register message count printer
signal.signal(signal.SIGINT, print_message_count_before_exit)

print('ProjectId: {}\nPub/Sub Topic: {}'.format(_GCP_PROJECT_ID, _TOPIC_PATH))
print('Sending events in background.')
print('Press Ctrl+C to exit/stop.')

# Infinite loop to keep sending messages to pub/sub
while _SEND_EVENTS_COUNT == -1 or message_count < _SEND_EVENTS_COUNT:
  event_message = build_message()
  if (_LOGGING_ENABLED):
    print('Sending Message {}\n{}'.format(message_count + 1,
                                          json.dumps(event_message)))

  message_count += 1
  pub_sub_message_unique_id = send_to_pub_sub(event_message)

  if (_LOGGING_ENABLED):
    print(
        'pub_sub_message_id: {}'.format(pub_sub_message_unique_id.result()))

  _sleep_time = random.randint(10, 10000)  # Random sleep time in milli-seconds.
  if (_LOGGING_ENABLED):
    print('Sleeping for {} ms'.format(_sleep_time))
  time.sleep(_sleep_time / 1000)

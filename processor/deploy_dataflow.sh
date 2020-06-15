#!/usr/bin/env bash
#
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
#

mvn clean compile exec:java \
  -Dexec.mainClass=com.google.cloud.solutions.realtimedash.pipeline.MetricsCalculationPipeline \
  -Dexec.cleanupDaemonThreads=false \
  -Dmaven.test.skip=true \
  -Dexec.args=" \
--project=$PROJECT_ID \
--runner=DataflowRunner \
--stagingLocation=gs://$TEMP_GCS_BUCKET/stage/ \
--tempLocation=gs://$TEMP_GCS_BUCKET/temp/ \
--inputTopic=projects/$PROJECT_ID/topics/$APP_EVENTS_TOPIC \
--workerMachineType=n1-standard-4 \
--region=$REGION_ID \
--subnetwork=regions/$REGION_ID/subnetworks/$VPC_NETWORK_NAME \
--redisHost=$REDIS_IP \
--redisPort=6379  \
--streaming\
"

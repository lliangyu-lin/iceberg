#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BASE_PATH := docker/iceberg-test-fixtures

ENGINE ?= spark-4.0.0
STORAGE ?= minio
CATALOG ?= rest

ENGINE_NAME := $(word 1,$(subst -, ,$(ENGINE)))
ENGINE_VERSION := $(word 2,$(subst -, ,$(ENGINE)))
CONNECTOR_VERSION := v$(shell echo $(ENGINE_VERSION) | cut -d. -f1,2)

ENGINE_BASE_IMAGE := apache/iceberg-engine-fixture-base
STORAGE_ENGINE_IMAGE := apache/iceberg-engine-storage-fixture-base
FULL_FIXTURE_IMAGE := apache/iceberg-engine-storage-catalog

# Build the iceberg engine base image
.PHONY: iceberg-engine
iceberg-engine:
	docker build \
	--build-arg ENGINE_VERSION=$(ENGINE_VERSION) \
	--build-arg CONNECTOR_VERSION=$(CONNECTOR_VERSION) \
	--tag $(ENGINE_BASE_IMAGE) \
	--file ${BASE_PATH}/${ENGINE_NAME}-base/Dockerfile .

# Build the iceberg engine image with storage
.PHONY: iceberg-engine-storage
iceberg-engine-storage: iceberg-engine
	docker build \
		  --tag $(STORAGE_ENGINE_IMAGE) \
		  --file ${BASE_PATH}/${STORAGE}/Dockerfile .

# Build the iceberg engine image with storage and catalog
.PHONY: iceberg-engine-storage-catalog
iceberg-engine-storage-catalog: iceberg-engine-storage
	docker build \
		--tag $(FULL_FIXTURE_IMAGE) \
		--file ${BASE_PATH}/${CATALOG}/Dockerfile .
	docker tag $(FULL_FIXTURE_IMAGE) $(FULL_FIXTURE_IMAGE):latest-$(ENGINE_NAME)-$(ENGINE_VERSION)-$(STORAGE)-$(CATALOG)

.PHONY: clean
clean:
	docker rmi $(ENGINE_BASE_IMAGE) || true
	docker rmi $(STORAGE_ENGINE_IMAGE) || true
	docker rmi $(FULL_FIXTURE_IMAGE) || true
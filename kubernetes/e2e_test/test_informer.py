# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import unittest
import uuid
from functools import partial
from datetime import timedelta
import time

from kubernetes import informer
from kubernetes.client import api_client
from kubernetes.client.api import core_v1_api
from kubernetes.e2e_test import base


def short_uuid():
    id = str(uuid.uuid4())
    return id[-12:]


def config_map_with_value(name, value):
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
        },
        "data": {
            "key": value,
            "config": "dummy",
        }
    }


class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = base.get_e2e_configuration()

    def setUp(self) -> None:
        client = api_client.ApiClient(configuration=self.config)
        self.api = core_v1_api.CoreV1Api(client)

        self.api.delete_collection_namespaced_config_map(
            namespace='default')

    def tearDown(self) -> None:
        self.api.delete_collection_namespaced_config_map(
            namespace='default')

    def test_informer_create_and_destroy(self):

        with informer.Informer(self.api.list_namespaced_config_map, namespace='default', relist_period=timedelta(seconds=1)) as i:
            # create a configmap
            name_a = 'configmap-a-' + short_uuid()

            # This configmap does not already exist.
            assert name_a not in resource_names(i.items)

            configmap_a = config_map_with_value(name_a, "a")
            self.api.create_namespaced_config_map(body=configmap_a, namespace='default')

            # The configmap appears in the informer collection
            wait_for_items(i, {name_a})
            assert name_a in resource_names(i.items)

            # Delete all configmaps
            self.api.delete_collection_namespaced_config_map(namespace='default')

            # The configmap disappears from the informer collection
            wait_for_items(i, set())
            assert name_a not in resource_names(i.items)

    def test_informer_reconnects(self):

        with informer.Informer(self.api.list_namespaced_config_map, namespace='default', relist_period=timedelta(seconds=1)) as i:

            time.sleep(2)

            # create a configmap
            name_a = 'configmap-a-' + short_uuid()

            # This configmap does not already exist.
            assert name_a not in resource_names(i.items)

            configmap_a = config_map_with_value(name_a, "a")
            self.api.create_namespaced_config_map(body=configmap_a, namespace='default')

            # The configmap appears in the informer collection
            wait_for_items(i, {name_a})
            assert name_a in resource_names(i.items)

            # Delete all configmaps
            self.api.delete_collection_namespaced_config_map(namespace='default')

    def test_extra_stop_ok(self):
        i = informer.Informer(self.api.list_namespaced_config_map, namespace='default', relist_period=timedelta(seconds=60))
        i.start()
        assert i.is_alive()
        i.stop()
        i.stop()

    def test_threads_can_only_be_started_once(self):
        i = informer.Informer(self.api.list_namespaced_config_map, namespace='default', relist_period=timedelta(seconds=60))
        i.start()

        with self.assertRaises(RuntimeError):
            i.start()

    def test_threads_can_only_be_started_once_context(self):
        i = informer.Informer(self.api.list_namespaced_config_map, namespace='default', relist_period=timedelta(seconds=60))

        with i:
            with self.assertRaises(RuntimeError):
                with i:
                    pass


def resource_names(items):
    return set([_.metadata.name for _ in items])


def wait_for_items(obj, expected, attribute="items"):
    have = None
    for attempt in range(100):
        have = resource_names(getattr(obj, attribute))
        print("Checking attempt", attempt, have)
        if expected == have:
            return attempt
        time.sleep(0.1)
    raise ValueError("Have (%s) != expected (%s)" % (have, expected))

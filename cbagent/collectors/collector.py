import socket
import sys
import time
from threading import Thread

import requests
from logger import logger

from cbagent.stores import SerieslyStore
from cbagent.metadata_client import MetadataClient


class Collector(object):

    COLLECTOR = None

    def __init__(self, settings):
        self.session = requests.Session()

        self.interval = settings.interval

        self.cluster = settings.cluster
        self.master_node = settings.master_node
        self.auth = (settings.rest_username, settings.rest_password)

        self.buckets = settings.buckets
        self.hostnames = settings.hostnames
        self.nodes = list(self.get_nodes())
        self.ssh_username = getattr(settings, 'ssh_username', None)
        self.ssh_password = getattr(settings, 'ssh_password', None)

        self.store = SerieslyStore(settings.seriesly_host)
        self.mc = MetadataClient(settings)

        self.metrics = set()
        self.updater = None

    def get_http(self, path, host_port=None):
        host_port = host_port or self.master_node
        host, port = host_port.split(':')
        url = "http://{}:{}{}".format(host, port, path)
        try:
            r = self.session.get(url=url, auth=self.auth)
            if r.status_code in (200, 201, 202):
                return r.json()
            else:
                logger.warn("Bad response: {}".format(url))
                return self.retry(path, host_port)
        except requests.ConnectionError:
            logger.warn("Connection error: {}".format(url))
            return self.retry(path, host_port)

    def retry(self, path, host_port=None):
        time.sleep(self.interval)
        for node in self.nodes:
            if self._check_node(node):
                self.master_node = node
                self.nodes = list(self.get_nodes())
                break
        else:
            logger.interrupt("Failed to find at least one node")
        if host_port not in self.nodes:
            raise RuntimeError("Bad node {}".format(host_port or ""))
        else:
            return self.get_http(path, host_port)

    def _check_node(self, node):
        try:
            host, port = node.split(':')
            s = socket.socket()
            s.connect((host, port))
        except socket.error:
            return False
        else:
            if not self.get_http(path="/pools", host_port=node).get("pools"):
                return False
        return True

    def get_buckets(self, with_stats=False):
        buckets = self.get_http(path="/pools/default/buckets")
        if not buckets:
            buckets = self.retry(path="/pools/default/buckets")
        for bucket in buckets:
            if self.buckets is not None and bucket["name"] not in self.buckets:
                continue
            if with_stats:
                yield bucket["name"], bucket["stats"]
            else:
                yield bucket["name"]

    def get_nodes(self):
        pool = self.get_http(path="/pools/default")
        for node in pool["nodes"]:
            hostname = node["hostname"].split(":")[0]
            if self.hostnames is not None and hostname not in self.hostnames:
                continue
            yield node["hostname"]

    def _update_metric_metadata(self, metrics, bucket=None, server=None):
        for metric in metrics:
            metric = metric.replace('/', '_')
            metric_hash = hash((metric, bucket, server))
            if metric_hash not in self.metrics:
                self.metrics.add(metric_hash)
                self.mc.add_metric(metric, bucket, server, self.COLLECTOR)

    def update_metric_metadata(self, *args, **kwargs):
        if self.updater is None or not self.updater.is_alive():
            self.updater = Thread(
                target=self._update_metric_metadata, args=args, kwargs=kwargs
            )
            self.updater.daemon = True
            self.updater.start()

    def sample(self):
        raise NotImplementedError

    def collect(self):
        while True:
            try:
                self.sample()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                logger.warn(e)

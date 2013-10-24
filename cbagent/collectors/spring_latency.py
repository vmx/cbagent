from time import time
from logger import logger

from spring.docgen import ExistingKey, NewDocument
from spring.querygen import NewQuery
from spring.cbgen import CBGen
from spring.tuqgen import NewTuq, NewCBQuery
from spring.tuqclient import TuqClient

from cbagent.collectors import Latency


class SpringLatency(Latency):

    COLLECTOR = "spring_latency"

    METRICS = ("latency_set", "latency_get")

    def __init__(self, settings, workload, prefix=None):
        super(Latency, self).__init__(settings)
        self.clients = []
        for bucket in self.get_buckets():
            client = CBGen(bucket=bucket, host=settings.master_node,
                           username=bucket, password=settings.rest_password)
            self.clients.append((bucket, client))

        self.existing_keys = ExistingKey(workload.working_set,
                                         workload.working_set_access,
                                         prefix=prefix)
        self.new_docs = NewDocument(workload.size)
        self.items = workload.items

    def measure(self, client, metric):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)
        doc = self.new_docs.next(key)

        t0 = time()
        if metric == "latency_set":
            client.create(key, doc)
        else:
            client.read(key)
        return 1000 * (time() - t0)  # Latency in ms

    def sample(self):
        for bucket, client in self.clients:
            samples = {}
            for metric in self.METRICS:
                samples[metric] = self.measure(client, metric)
            self.store.append(samples, cluster=self.cluster,
                              bucket=bucket, collector=self.COLLECTOR)


class SpringQueryLatency(SpringLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = ("latency_query", )

    def __init__(self, settings, workload, ddocs, prefix=None):
        super(SpringQueryLatency, self).__init__(settings, workload, prefix)
        self.new_queries = NewQuery(ddocs)

    def measure(self, client, metric):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)
        doc = self.new_docs.next(key)
        ddoc_name, view_name, query = self.new_queries.next(doc)

        t0 = time()
        client.query(ddoc_name, view_name, query=query)
        return 1000 * (time() - t0)

class SpringTuqLatency(SpringLatency):
    COLLECTOR = "spring_tuq_latency"

    METRICS = ("latency_tuq", "latency_query")

    def __init__(self, settings, workload, indexes, tuq_server, prefix=None):
        super(Latency, self).__init__(settings)
        self.clients = []
        self.bucket = list(self.get_buckets())[0]
        self.tuq_client = TuqClient(bucket=self.bucket, host=settings.master_node,
                                    username=settings.rest_username,
                                    password=settings.rest_password,
                                    tuq_server=tuq_server)
        self.cb_client = CBGen(bucket=self.bucket, host=settings.master_node,
                               username=self.bucket, password=settings.rest_password)

        self.existing_keys = ExistingKey(workload.working_set,
                                         workload.working_set_access,
                                         prefix=prefix)
        self.new_docs = NewDocument(workload.size)
        self.items = workload.items
        self.new_tuqs = NewTuq(indexes, self.bucket)
        self.new_queries = NewCBQuery(indexes, self.bucket)

    def _measure(self, metric):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)
        doc = self.new_docs.next(key)
        tuq = self.new_tuqs.next(doc)
        ddoc_name, view_name, query = self.new_queries.next(doc)

        t0 = time()
        if metric == "latency_tuq":
            self.tuq_client.query(tuq)
        else:
            logger.info("Firing cb query: %s, %s, %s" % (ddoc_name, view_name, query))
            self.cb_client.query(ddoc_name, view_name, query=query)
        return 1000 * (time() - t0)

    def sample(self):

        samples = {}
        for metric in self.METRICS:
            samples[metric] = self._measure(metric)
        self.store.append(samples, cluster=self.cluster,
                          bucket=self.bucket, collector=self.COLLECTOR)
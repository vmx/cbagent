from fabric.api import run

from cbagent.collectors.libstats.remotestats import (
    RemoteStats, multi_node_task)


class TPStats(RemoteStats):

    METRICS = (
        ("rss", 1),    # already in bytes
    )

    def __init__(self, hosts, user, password):
        super(TPStats, self).__init__(hosts, user, password)
        self.typeperf_cmd = "typeperf \"\\Process(*erl*)\\Working Set\" -sc 1|sed '3q;d'"

    @multi_node_task
    def get_samples(self, process):
        samples = {}
        stdout = run(self.typeperf_cmd)
        values = stdout.split(',')[1:5]
        sum_rss = 0
        if stdout:
            for v in values:
                v = float(v.replace('"',''))
                sum_rss = sum_rss + v
            metric, multiplier = self.METRICS[0]
            title = "{}_{}".format(process, metric)
            samples[title] = float(sum_rss) * multiplier
            return samples

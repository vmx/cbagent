from cbagent.collectors import Collector
import os.path

class SecondaryLatencyStats(Collector):

    COLLECTOR = "secondaryscan_latency"
    fname = self.secondary_statsfile

    def _get_secondaryscan_latency(self):
        stats = {}
        if os.path.isfile(self.secondary_statsfile):
            with open(fname, 'rb') as fh:
                first = next(fh).decode()
                fh.seek(-200, 2)
                last = fh.readlines()[-1].decode()
                duration = last.split(',')[-1]
                stats = {}
                stats[duration.split(':')[0]] = duration.split(':')[1]
        return stats

    def sample(self):
        stats = self._get_secondaryscan_latency()
        if stats:
            self.update_metric_metadata(stats.keys())
            self.store.append(stats, cluster=self.cluster, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

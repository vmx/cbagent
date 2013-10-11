import re
from uuid import uuid4

from fabric.api import run

from cbagent.collectors.libstats.remotestats import (
    RemoteStats, multi_node_task, single_node_task)


class AtopStats(RemoteStats):

    def __init__(self, hosts, user, password):
        super(AtopStats, self).__init__(hosts, user, password)
        self.logfile = "/tmp/{0}.atop".format(uuid4().hex)

        self._base_cmd =\
            "d=`date +%H:%M` && atop -r {0} -b $d -e $d".format(self.logfile)

    @multi_node_task
    def stop_atop(self):
        run("killall -q atop")
        run("rm -rf /tmp/*.atop")

    @multi_node_task
    def start_atop(self):
        run("nohup atop -a -w {0} 5 > /dev/null 2>&1 &".format(self.logfile),
            pty=False)

    @single_node_task
    def update_columns(self):
        self._cpu_column = self._get_cpu_column()
        self._vsize_column = self._get_vsize_column()
        self._rss_column = self._get_rss_column()
        self._get_disk_columns()
        self._disk_flags = self._get_disk_flags()

    def restart_atop(self):
        self.stop_atop()
        self.start_atop()

    @single_node_task
    def _get_vsize_column(self):
        output = run("atop -m 1 1 | grep PID")
        return output.split().index("VSIZE")

    @single_node_task
    def _get_rss_column(self):
        output = run("atop -m 1 1 | grep PID")
        return output.split().index("RSIZE")

    @single_node_task
    def _get_cpu_column(ip):
        output = run("atop 1 1 | grep PID")
        return output.split().index("CPU")

    def add_disk_metrics(self, metrics):
        for disk in self._disk_flags:
            metrics = metrics + ("%s_read_KB_r" % disk, "%s_write_KB_w" % disk)
            metrics = metrics + ("%s_read_MB_s" % disk, "%s_write_MB_s" % disk)
            metrics = metrics + ("%s_busy_percent" % disk,"%s_avq_size" % disk)

        return metrics

    def get_disk_flags(self):
        return self._disk_flags

    @single_node_task
    def _get_disk_flags(self):
        output = run("atop -d -f -L200 1 1 | grep 'DSK |'")
        return map(lambda row: row.split("|")[self._disk_flag_column].strip(),
                   output.split("\n"))

    @single_node_task
    def _get_disk_columns(self):
        output = run("atop -d -f -L200 1 1 | grep 'DSK |' | sed -n 1p")
        cols = output.split("|")

        p = re.compile(".*sd.*|.*dm.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_flag_column = cols.index(matches[0]) if matches else 1

        p = re.compile(".*KiB/r.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_read_KB_column = cols.index(matches[0]) if matches else 5

        p = re.compile(".*KiB/w.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_write_KB_column = cols.index(matches[0]) if matches else 6

        p = re.compile(".*MBr/s.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_read_MB_column = cols.index(matches[0]) if matches else 7

        p = re.compile(".*MBw/s.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_write_MB_column = cols.index(matches[0]) if matches else 8

        p = re.compile(".*busy.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_busy_column = cols.index(matches[0]) if matches else 2

        p = re.compile(".*avq.*")
        matches = filter(lambda col: p.match(col), cols)
        self._disk_avq_column = cols.index(matches[0]) if matches else 9

    @multi_node_task
    def get_process_cpu(self, process):
        title = process + "_cpu"
        cmd = self._base_cmd + "| grep {0}".format(process)
        output = run(cmd)
        return title, output.split()[self._cpu_column]

    @multi_node_task
    def get_process_vsize(self, process):
        title = process + "_vsize"
        cmd = self._base_cmd + " -m | grep {0}".format(process)
        output = run(cmd)
        return title, output.split()[self._vsize_column]

    @multi_node_task
    def get_process_rss(self, process):
        title = process + "_rss"
        cmd = self._base_cmd + " -m | grep {0}".format(process)
        output = run(cmd)
        return title, output.split()[self._rss_column]

    @multi_node_task
    def get_disk_stats(self, disk):
        cmd = self._base_cmd + " -d -f -L200 | grep 'DSK |' | grep {0}".format(disk)
        output = run(cmd)
        if output.return_code != 0:
            self._disk_flags.remove(disk)
            return {}
        t_read_KB = "%s_read_KB_r" % disk
        t_write_KB = "%s_write_KB_w" % disk
        t_read_MB = "%s_read_MB_s" % disk
        t_write_MB = "%s_write_MB_s" % disk
        t_busy_percent = "%s_busy_percent" % disk
        t_avq_size = "%s_avq_size" % disk
        return {t_read_KB: output.split("|")[self._disk_read_KB_column].split()[1],
                t_write_KB: output.split("|")[self._disk_write_KB_column].split()[1],
                t_read_MB: output.split("|")[self._disk_read_MB_column].split()[1],
                t_write_MB: output.split("|")[self._disk_write_MB_column].split()[1],
                t_busy_percent: output.split("|")[self._disk_busy_column].split()[1],
                t_avq_size: output.split("|")[self._disk_avq_column].split()[1]}

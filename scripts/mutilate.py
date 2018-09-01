#!/usr/bin/python3
#
# Copyright 2018 The Sphinxd Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import argparse
import time
import sys
import re
import os


class Runner:

    def __init__(self, args):
        self.args = args
        self.ssh_cmd = ["ssh", args.server_host]

        self.server_cmd = list(self.ssh_cmd)
        if args.server_cpu_affinity:
            self.server_cmd += ["taskset",
                                "--cpu-list", args.server_cpu_affinity]
        self.server_cmd += [args.server_cmd]
        self.server_cmd += ['-p', str(args.server_tcp_port)]
        self.server_cmd += ['-t', str(args.server_threads)]
        self.server_cmd += ['-m', str(args.server_memory)]
        if args.server_cpu_isolate:
            self.server_cmd += ['-i', args.server_cpu_isolate]

        self.server_pattern = os.path.basename(args.server_cmd)
        self.pkill_cmd = self.ssh_cmd + ["pkill", self.server_pattern]

    def start_server(self):
        print("# server command = %s" % ' '.join(self.server_cmd))
        if not self.args.dry_run:
            server_proc = subprocess.Popen(self.server_cmd, stdout=open(
                '/dev/null', 'r'), stderr=open('/dev/null', 'r'))
            time.sleep(int(self.args.server_startup_wait))

    def kill_server(self):
        print("# pkill command = %s" % ' '.join(self.pkill_cmd))
        if not self.args.dry_run:
            subprocess.call(self.pkill_cmd, stdout=open(
                '/dev/null', 'r'), stderr=open('/dev/null', 'r'))


def measure(args):
    if args.scenario == 'read':
        update_ratio = 0.0
    elif args.scenario == 'update':
        update_ratio = 1.0
    else:
        raise ValueError("unable to parse scenario: '%s'" % args.scenario)

    raw_client_conn_range = args.client_connections.split(',')
    if len(raw_client_conn_range) == 1:
        client_conn_range = range(int(raw_client_conn_range[0]))
    elif len(raw_client_conn_range) == 2:
        client_conn_range = range(
            int(raw_client_conn_range[0]), int(raw_client_conn_range[1]))
    elif len(raw_client_conn_range) == 3:
        client_conn_range = range(int(raw_client_conn_range[0]), int(
            raw_client_conn_range[1]), int(raw_client_conn_range[2]))
    else:
        raise ValueError(
            "unable to parse client connection range: '%s' % args.client_connections")

    output_file = open(args.output, "w")

    outputs = [sys.stdout, output_file]

    for out in outputs:
        out.write(
            "Sample\tConcurrency\tavg\tstd\tmin\tp1\tp5\tp10\tp20\tp50\tp80\tp90\tp95\tp99\tQPS\tRX\tTX\n")
        out.flush()

    runner = Runner(args)

    for client_conn in client_conn_range:
        for sample in range(0, args.samples):
            runner.start_server()

            client_cmd = [args.client_cmd]
            client_cmd += ['-s', "%s:%d" %
                           (args.server_host, args.server_tcp_port)]
            client_cmd += ['-T', str(args.client_threads)]
            client_cmd += ['-u', str(update_ratio)]
            client_cmd += ['-t', str(args.duration)]
            client_cmd += ['-c', str(client_conn)]
            print("# client command = %s" % ' '.join(client_cmd))

            if not args.dry_run:
                raw_client_output = subprocess.check_output(client_cmd)
                client_output = str(raw_client_output)

                latency_regex = r"%s\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+.\d+)" % args.scenario
                avg = re.search(latency_regex, client_output).group(1)
                std = re.search(latency_regex, client_output).group(2)
                min_ = re.search(latency_regex, client_output).group(3)
                p1 = re.search(latency_regex, client_output).group(4)
                p5 = re.search(latency_regex, client_output).group(5)
                p10 = re.search(latency_regex, client_output).group(6)
                p20 = re.search(latency_regex, client_output).group(7)
                p50 = re.search(latency_regex, client_output).group(8)
                p80 = re.search(latency_regex, client_output).group(9)
                p90 = re.search(latency_regex, client_output).group(10)
                p95 = re.search(latency_regex, client_output).group(11)
                p99 = re.search(latency_regex, client_output).group(12)

                qps_regex = r"Total QPS = (\d+.\d+)"
                qps = re.search(qps_regex, client_output).group(1)

                rx_regex = r"RX\s+\d+ bytes :\s+(\d+.\d+) MB/s"
                rx = re.search(rx_regex, client_output).group(1)

                tx_regex = r"TX\s+\d+ bytes :\s+(\d+.\d+) MB/s"
                tx = re.search(tx_regex, client_output).group(1)

                result = "%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
                    sample + 1, client_conn, avg, std, min_, p1, p5, p10, p20, p50, p80, p90, p95, p99, qps, rx, tx)

                for out in outputs:
                    out.write(result)
                    out.flush()

            runner.kill_server()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark Memcache server using Mutilate tool")
    parser.add_argument("--server-host", metavar="HOST", type=str,
                        required=True, help="host name of the server to benchmark")
    parser.add_argument("--server-startup-wait", metavar="TIME", type=int, required=False,
                        default=5, help="time to wait for server to start up before starting benchmark")
    parser.add_argument("--server-cmd", metavar='CMD', type=str,
                        required=True, help="command to start the server with")
    parser.add_argument("--server-tcp-port", metavar='PORT', type=int, required=False,
                        default=11211, help="TCP port to listen on (default: 11211)")
    parser.add_argument("--server-threads", metavar='N', type=int,
                        required=True, help="number of server threads to use")
    parser.add_argument("--server-memory", metavar='SIZE', type=int,
                        required=True, help="amount of server memory to use in megabytes")
    parser.add_argument("--server-cpu-affinity", metavar='LIST', type=str, required=False, default=None,
                        help="list of processor to run server threads on (default: disabled). For example, use '--server-cpu-affinity 0,2-3', to run server threads on CPUs 0, 2, and 3.")
    parser.add_argument("--server-cpu-isolate", metavar='LIST', type=str, required=False, default=None,
                        help="list of processor to isolate server threads from (default: disabled). For example, use '--server-cpu-isolate 0,2-3', to not run server threads on CPUs 0, 2, and 3.")
    parser.add_argument("--client-cmd", metavar='CMD', type=str, required=True,
                        help="command to start the client with. For example, use '--client-cmd ./mutilate' to start 'mutilate' executable from local diretory")
    parser.add_argument("--client-threads", metavar='N', type=int,
                        required=True, help="number of client threads to use")
    parser.add_argument("--client-connections", metavar='RANGE', type=str, required=True,
                        help="range of number of client connections to use. For example, use '--client-connections 10,21,5', to run with 10, 15, and 20 client connections.")
    parser.add_argument("--samples", metavar='N', type=int, required=True,
                        help="number of samples to measure per client connection count")
    parser.add_argument("--scenario", metavar='NAME', type=str, required=True,
                        help="name of benchmark scenario to run (options: 'read' and 'update'")
    parser.add_argument("--duration", metavar='TIME', type=int, required=True,
                        help="duration to run the measurements for in seconds")
    parser.add_argument("--dry-run", action="store_true",
                        help="print commands to be executed but don't run them")
    parser.add_argument("--output", type=str,
                        required=True, help="Output file")
    return parser.parse_args()


def main():
    args = parse_args()

    measure(args)

if __name__ == '__main__':
    main()

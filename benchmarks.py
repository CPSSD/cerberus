#!/usr/bin/env python3
import atexit
import re
import time
import os
import pdb
import shutil
import signal
import subprocess

import numpy as np
import matplotlib.pyplot as plt

path = os.environ["PWD"] + "/target/debug/"
master_cmd = "./target/debug/master -p 1234 -s " + path + "benchmarks --fresh"
worker_cmd = "./target/debug/worker -m [::]:1234"
opened_pids = []

@atexit.register
def kill_children():
    for pid in opened_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception as e:
            pass

# Build the binaries.
proc = subprocess.Popen("cargo build --all".split(" "))

# Add tesdata to input directory.
line = "how much wood would a wood chuck chuck if a wood chuck could chuck wood"

try:
    os.mkdir(path + "benchmarks")
except FileExistsError:
    shutil.rmtree(path + "benchmarks")
    os.mkdir(path + "benchmarks")
os.mkdir(path + "benchmarks/input")
os.mkdir(path + "benchmarks/logs")
input_file = open(path + "benchmarks/input/input.txt", "w+")

# Write the sentance to the input_file multiple times.
for i in range(0, 100000):
    input_file.write(line)
input_file.close()

def launch_master():
    master_proc = subprocess.Popen(
            master_cmd.split(" "),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    opened_pids.append(master_proc.pid)
    return master_proc

def launch_workers(count):
    workers = []
    for i in range(count):
        worker_stdout = open(path + "benchmarks/logs/worker_{}.log".format(i), "w+")
        worker_proc = subprocess.Popen(
                worker_cmd.split(" "),
                stdout=worker_stdout,
                stderr=worker_stdout)
        opened_pids.append(worker_proc.pid)
        workers.append(worker_proc)
    return workers

def queue_mapreduce():
    cli_cmd = "./target/debug/cli -m [::]:1234 run -b {} -i {} -o {}".format(
            path + "examples/word-counter",
            path + "benchmarks/input",
            path + "benchmarks/output")
    cli_stdout = open(path + "benchmarks/logs/cli_queue.log", "w+")
    return subprocess.run(cli_cmd.split(" "), stdout=cli_stdout, stderr=cli_stdout)

def get_cpu_time(master_proc):
    master_proc.terminate()
    opened_pids.remove(master_proc.pid)
    stdout, stderr = master_proc.communicate()
    match = re.search(b'Total CPU time used: ([0-9]*)', stdout + stderr)
    if match:
        return int(match.group(1))
    return 0

# Wait for completion.
completion_cmd = "./target/debug/cli -m [::]:1234 status"
def poll_results(master_proc):
    cli_proc = subprocess.run(completion_cmd.split(" "), stdout=subprocess.PIPE)

    # Get time taken
    time_taken = re.search(b'\(([0-9]*)s\)', cli_proc.stdout)
    if not time_taken:
        print("Unable to get time taken..")
        return (0, 0)
    time_taken = int(time_taken.group(1))

    done = re.search(b'DONE ([^)]*)', cli_proc.stdout)
    if done:
        cpu_time = get_cpu_time(master_proc)
        print("Results: Time Taken: {}, CPU Time: {}".format(time_taken, cpu_time))
        return (time_taken, cpu_time)

    time.sleep(2)
    return poll_results(master_proc)


def run(worker_count):
    master_proc = launch_master()
    workers = launch_workers(worker_count)
    queue_mapreduce()
    print("* Running with {} workers".format(worker_count))
    time_taken, cpu_time = poll_results(master_proc)
    cleanup()
    return (worker_count, time_taken, cpu_time)

def cleanup():
    global opened_pids
    for pid in opened_pids:
        os.kill(pid, signal.SIGTERM)
    opened_pids = []

def plot_results(results):
    workers  = [r[0] for r in results]
    time     = [r[1] for r in results]
    cpu_time = [r[2] for r in results]

    # Plot workers * time
    plt.figure(1)
    plt.title("Workers * Time")
    plt.xlabel("Workers")
    plt.ylabel("Time Taken (seconds)")
    plt.plot(workers, time, 'r-', linewidth=1)
    plt.xlabel

    # Plot workers * cpu_time
    plt.figure(2)
    plt.title("Workers * CPU Time")
    plt.xlabel("Workers")
    plt.ylabel("CPU Time")
    plt.plot(workers, cpu_time, "b-", linewidth=1)

    plt.show()

results = [ run(i) for i in [1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 40, 50] ]
plot_results(results)
print("Results: " + results)

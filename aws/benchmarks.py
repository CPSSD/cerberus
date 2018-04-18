#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import sys
import time

import matplotlib.pyplot as plt

import aws

BENCHMARK_STEP = 3
INPUT_LOCATION = "/input/"
BINARY_LOCATION = "/executables/rating-aggregator"
OUTPUT_LOCATION = "/benchmark-output"
MAP_SIZE = "8"

# Setup our argument parser.
parser = argparse.ArgumentParser(description='Benchmarking on AWS', prog="benchmarks.py")
parser.add_argument('-s', '--starting_workers', type=int, default=1, action='store',
        help="The minimum ammount of workers to start with")
parser.add_argument('-m', '--max_workers', type=int, default=1, action='store',
        help="The maximum ammount of workers to run with")
args = parser.parse_args()

completion_cmd = "../target/release/cli -m %s:8081 status"
failed_polls = 0

def poll_results():
    while True:
        cli_proc = subprocess.run(completion_cmd.split(" "), stdout=subprocess.PIPE)

        # Get time taken
        time_taken = re.search(b'\(([0-9]*)s\)', cli_proc.stdout)
        global failed_polls
        if not time_taken:
            failed_polls += 1
            if failed_polls >= 100:
                print("Failed to get time taken 100 times. Exiting")
                sys.exit(1)
            print("Unable to get time taken..")
            time.sleep(2)
            continue

        failed_polls = 0
        time_taken = int(time_taken.group(1))

        done = re.search(b'DONE ([^)]*)', cli_proc.stdout)
        if done:
            print("Results: Time Taken: {}".format(time_taken))
            return time_taken

        time.sleep(2)

def plot_results(results):
    workers  = [r[0] for r in results]
    time     = [r[1] for r in results]

    # Plot workers * time
    plt.figure(1)
    plt.title("Workers * Time")
    plt.xlabel("Workers")
    plt.ylabel("Time Taken (seconds)")
    plt.plot(workers, time, 'r-', linewidth=1)

    plt.show()

def benchmark(start, end, master_ip, abstraction_layer):
    # Iterate from start_workers to max_workers (inclusive)
    workerRange = [i for i in range(start, end, BENCHMARK_STEP)] + [ end ]

    results = []
    current_workers = args.starting_workers
    for n in workerRange:
        workers_required = n - current_workers

        print("\t** Running benchmark for %d workers **" % n)

        if workers_required != 0:
            print("\t** Starting up %d new instances **" % workers_required)
            aws.create_instances(workers_required, workers_only=True)
            current_workers += workers_required

        # Deploy the containers on N workers
        print("\t\t-- Deploying Containers")
        aws.deploy_containers(abstraction_layer, n, None)

        # Launch the benchmarking test
        cmd = [ "../target/release/cli",
                "-m", "%s:8081" % master_ip, "run",
                "-i", INPUT_LOCATION,
                "-o", OUTPUT_LOCATION,
                "-b", BINARY_LOCATION,
                "-m", MAP_SIZE ]
        print("\t\t-- Starting MapReduce ")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        time.sleep(5)

        # Poll & Measure results
        print("\t\t-- Polling Results")
        runtime = poll_results()
        results.append([n, runtime])

        # Terminate running containers
        print("\t\t-- Terminating containers")
        aws.terminate_containers(remove_images=False)
    return results

if __name__ == "__main__":
    print("Running AWS benchmarks")

    ## Make sure we have no instances running
    print("\t** Killing running instances **")
    aws.kill_instances()
    time.sleep(3)

    print("\t** Starting up %d instances **" % args.starting_workers)
    master_ip = aws.create_instances(args.starting_workers, workers_only=False)
    completion_cmd = completion_cmd % master_ip

    print("\t** Launching Benchmarks **")
    results = benchmark(args.starting_workers, args.max_workers, master_ip, "S3")

    ## Kill running instances?
    print("\t** Killing running instances **")
    aws.kill_instances()

    print("\t** Plotting results **")
    plot_results(results)

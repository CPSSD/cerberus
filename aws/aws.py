#!/usr/bin/python

import argparse
import time
import subprocess

import boto3

AWS_REGION = "eu-west-1"
MASTER_WEB_PORT = 80
MASTER_PORT = 8083
WORKER_PORT = 3003
DOCKER_REPO = "ryanconnell/cerberus-production"
PATH_TO_SSH_KEY = "key.pem"

# Setup our argument parser.
parser = argparse.ArgumentParser(description='Manage AWS Deployments. Arguments ' \
        'can be chained and will be run in the order displayed below.',
        prog="aws-deploy.py")
parser.add_argument('-k', '--kill', action='store_true',
        help="Terminate all running servers.")
parser.add_argument('-c', '--create', type=int, metavar='N',
        help="Create servers. 1 Master and N workers.")
parser.add_argument('-t', '--terminate', action='store_true',
        help="Terminates containers on all running servers.")
parser.add_argument('-d', '--deploy', action='store_true',
        help="Deploys containers on all running servers.")
args = parser.parse_args()

# Create our EC2 interface
ec2 = boto3.resource('ec2', region_name=AWS_REGION)
ec2_client = boto3.client('ec2', region_name=AWS_REGION)

# Setup commands for various tasks.
docker_command = "sudo yum install docker -y && sudo service docker start"

common_tmpl = 'sudo docker run -d -e MASTER_IP="%%s" -e MASTER_PORT="%s"' % MASTER_PORT
master_tmpl = '%s -p %d:8081 -p %d:8082 %s:latest-master' % (
        common_tmpl, MASTER_PORT, MASTER_WEB_PORT, DOCKER_REPO)
worker_tmpl = '%s -p %d:3000 -e WORKER_IP="%%s" %s:latest-worker' % (
        common_tmpl, WORKER_PORT, DOCKER_REPO)

def deploy_containers():
    instances = get_instances(["master", "worker"])
    wait_for_ips(instances["master"] + instances["worker"])

    if not instances['master']:
        print("Unable to deploy containers. No master instance was tagged.")
        return

    master = instances['master'][0]
    workers = instances['worker']

    worker_ips = [ worker.public_ip_address for worker in workers ]
    master_ip = master.public_ip_address

    # Setup master
    master_result = run_command(master_tmpl % master_ip, [master_ip],
                                hide_stdout=False, wait=True)

    # Setup workers
    worker_results = []
    for ip in worker_ips:
        result = run_command(worker_tmpl % (master_ip, ip), [ip], hide_stdout=False)
        worker_results += result

    for result in worker_results:
        result.wait()

    print(format("\n%d workers deployed\nMaster deployed at %s" %
                 (len(workers), master.public_ip_address)))

def terminate_containers():
    instances = get_instances(["master", "worker"])
    all_instances = instances["master"] + instances["worker"]
    for instance in all_instances:
        terminate_containers_on_instance(instance)

def terminate_containers_on_instance(instance):
    ip = instance.public_ip_address

    # Removes all containers
    container_list = "$(sudo docker ps -a -q)"
    cmd = "if [[ %s ]]; then sudo docker rm -f %s; fi" % (container_list, container_list)

    # Removes all images
    image_list = "$(sudo docker images -q)"
    cmd += "&& if [[ %s ]]; then sudo docker rmi -f %s; fi" % (image_list, image_list)

    run_command(cmd, [ip], hide_stdout=False, wait=True)

def kill_instances():
    instance_map = get_instances(["master", "worker"])
    instances = instance_map["master"] + instance_map["worker"]
    ids = [inst.id for inst in instances]

    print("Terminating instances with the following IDs:\n %s" % str(ids))
    ec2.instances.filter(InstanceIds=ids).terminate()

def create_instances(worker_count):
    print("Deploying 1 master and %d workers" % worker_count)

    # Launch 1 master instance
    master_response = ec2_client.run_instances(
        LaunchTemplate={
            'LaunchTemplateName': 'Master',
        },
        MaxCount=1,
        MinCount=1,
    )
    print ("Launched master")

    worker_response = ec2_client.run_instances(
        LaunchTemplate={
            'LaunchTemplateName': 'Worker',
        },
        MaxCount=worker_count,
        MinCount=worker_count,
    )
    print("Launched %d workers" % worker_count)

    # Wait for the instances to finish starting up.
    master_id = master_response['Instances'][0]['InstanceId']
    worker_ids = [ inst['InstanceId'] for inst in worker_response['Instances'] ]
    ids = worker_ids + [ master_id ]
    time.sleep(3)

    print("Waiting for %d instances to start" % len(ids))
    ips = []
    for inst in ec2.instances.filter(InstanceIds=ids):
        inst.wait_until_running()
        inst.reload()
        print("Instance %s is now running on ip %s" % (
            inst.id, inst.public_ip_address))
        ips += [ inst.public_ip_address ]

        # Attempt to connect via SSH. This won't work until the initialization stage
        # has completed.
        if not wait_for_ssh(inst.public_ip_address):
            print("Unable to connect to instance after 10 minutes.")
            return

    # Make sure Docker is installed and running on all hosts.
    docker_result = run_command(docker_command, ips, hide_stdout=True, wait=True)


def wait_for_ssh(ip):
    attempts = 0
    while(attempts < 200):
        cmd = [ "ssh", "-i", PATH_TO_SSH_KEY, "-o", "StrictHostKeyChecking=no",
                "ec2-user@%s" % ip, "echo 'cerberus'" ]
        result = subprocess.Popen(cmd, stdout=subprocess.PIPE)

        if result.communicate()[0] == "cerberus\n":
            return True
        time.sleep(3)

        attempts += 1
    return False

def wait_for_ips(instances):
    for instance in instances:
        while(instance.public_ip_address is None):
            print("Waiting for instance %s to get assigned a public ip address" % instance.id)
            time.sleep(3)
            instance.reload()

def run_command(command, ips, hide_stdout=False, wait=False):
    results = []
    for ip in ips:
        print("Attempting to execute command on " + ip + ": " + command)
        cmd = [ "ssh", "-i", PATH_TO_SSH_KEY, "-o", "StrictHostKeyChecking=no",
                "ec2-user@%s" % ip, command ]

        if hide_stdout:
            results += [ subprocess.Popen(cmd, stdout=subprocess.PIPE) ]
        else:
            results += [ subprocess.Popen(cmd) ]

    if wait:
        for result in results:
            result.wait()

    return results

def get_instances(inst_type):
    instances = {}
    for t in inst_type:
        instances[t] = []
    for instance in ec2.instances.all():
        if instance.state['Name'] in ['terminated', "shutting-down"]:
            continue
        tags = instance.tags or []
        for tag in tags:
            if tag["Key"] == "type" and tag["Value"] in inst_type:
                instances[tag["Value"]] += [ instance ]
    return instances

# Perform operations based on args.
if args.kill:
    kill_instances()

if args.create != None:
    create_instances(args.create)

if args.terminate:
    terminate_containers()

if args.deploy:
    deploy_containers()

#!/usr/bin/python

import argparse
import os
import subprocess
import time

import boto3

AWS_REGION = "eu-west-1"
MASTER_WEB_PORT = 80
MASTER_PORT = 8081
WORKER_PORT = 3000
DOCKER_REPO = "cerberuscpssd/cerberus-production"
S3_BUCKET = "cpssd-cerberus"
PATH_TO_SSH_KEY = "key.pem"
AWS_CREDENTIALS = "%s/.aws/credentials" % os.environ.get("HOME")

# Setup our argument parser.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage AWS Deployments. Arguments ' \
            'can be chained and will be run in the order displayed below.',
            prog="aws-deploy.py")
    parser.add_argument('-k', '--kill', action='store_true',
            help="Terminate all running servers.")
    parser.add_argument('-c', '--create', type=int, metavar='N',
            help="Create servers. 1 Master and N workers.")
    parser.add_argument('-t', '--terminate', action='store_true',
            help="Terminates containers on all running servers.")
    parser.add_argument('-d', '--deploy', type=str, choices=["DFS", "S3"],
            metavar='DATA_ABSTRACTION_LAYER', action='store',
            help="Deploys containers on all running servers. Valid abstraction layers " \
                 "are DFS and S3")
    parser.add_argument('-w', '--worker_limit', type=int, action='store', default=0)
    parser.add_argument('-m', '--master', type=str, action='store',
            help="Deployed workers will connect to this master instead of deploying " \
                 "a new one on AWS.")
    parser.add_argument('--workers_only', action='store_true', default=False,
            help="When this flag is set we will only create and deploy to workers" )
    parser.add_argument('--deployment_credentials', type=str, default=AWS_CREDENTIALS,
            help="Specifies the credentials to be used for deployment. Default value is " \
                 "the local credentials. (This flag is only useful when deploying for S3)")
    args = parser.parse_args()

# Create our EC2 interface
ec2 = boto3.resource('ec2', region_name=AWS_REGION)
ec2_client = boto3.client('ec2', region_name=AWS_REGION)

# Setup commands for various tasks.
docker_command = "sudo yum install docker -y && sudo service docker start"

common_tmpl = 'sudo docker run -d -e MASTER_IP="%%s" -e MASTER_PORT="%s" ' \
              '-e DATA_ABSTRACTION_LAYER="%%s" -e S3_BUCKET="%s" ' \
              '-v /home/ec2-user/.aws:/home/.aws -e ' \
              'AWS_SHARED_CREDENTIALS_FILE=/home/.aws/credentials ' \
              '-e SSL_CERT_DIR=/etc/ssl/certs' % (
                      MASTER_PORT, S3_BUCKET)
master_tmpl = '%s -p %d:8081 -p %d:8082 %s:latest-master' % (
        common_tmpl, MASTER_PORT, MASTER_WEB_PORT, DOCKER_REPO)
worker_tmpl = '%s -p %d:3000 -e WORKER_IP="%%s" %s:latest-worker' % (
        common_tmpl, WORKER_PORT, DOCKER_REPO)

def deploy_containers(data_abstraction_layer, worker_limit, master_ip,
                      credentials_path=AWS_CREDENTIALS):
    master_exists = master_ip != None

    required_instances = ["worker"]
    if not master_exists:
        required_instances += [ "master" ]

    instances = get_instances(required_instances)
    wait_for_ips(instances["worker"])
    if not master_exists:
        wait_for_ips(instances["master"])

    if not master_exists and not 'master' in instances:
        print("Unable to deploy containers. No master instance was tagged.")
        return

    workers = instances['worker']

    if worker_limit != 0 and len(workers) > worker_limit:
        workers = workers[:worker_limit]

    worker_ips = [ worker.public_ip_address for worker in workers ]
    master_ip = master_ip or instances['master'][0].public_ip_address

    # TODO: Come up with a more secure solution to this
    # If we are using S3 as our DataAbstractionLayer then we need to send over our
    # credentials.
    if data_abstraction_layer == "S3":
        if not master_exists:
            send_credentials([master_ip], credentials_path)
        send_credentials(worker_ips, credentials_path)

    # Setup master
    if not master_exists:
        master_result = run_command(master_tmpl % (master_ip, data_abstraction_layer),
                                    [master_ip], hide_stdout=False, wait=True)

    # Setup workers
    worker_results = []
    for ip in worker_ips:
        result = run_command(worker_tmpl % (master_ip, data_abstraction_layer, ip),
                             [ip], hide_stdout=False)
        worker_results += result

    for result in worker_results:
        result.wait()

    print(format("\n%d workers deployed" % len(workers)))
    if master_exists:
        print(format("Master was already deployed at %s" % master_ip))
    else:
        print(format("Master deployed at %s" % master_ip))

def terminate_containers(remove_images=True):
    instances = get_instances(["master", "worker"])
    all_instances = instances["master"] + instances["worker"]
    for instance in all_instances:
        terminate_containers_on_instance(instance, remove_images=remove_images)

def terminate_containers_on_instance(instance, remove_images):
    ip = instance.public_ip_address

    # Removes all containers
    container_list = "$(sudo docker ps -a -q)"
    cmd = "if [[ %s ]]; then sudo docker rm -f %s; fi" % (container_list, container_list)

    # Removes all images
    if remove_images:
        image_list = "$(sudo docker images -q)"
        cmd += "&& if [[ %s ]]; then sudo docker rmi -f %s; fi" % (image_list, image_list)

    run_command(cmd, [ip], hide_stdout=False, wait=True)

def kill_instances():
    instance_map = get_instances(["master", "worker"])
    instances = instance_map["master"] + instance_map["worker"]
    ids = [inst.id for inst in instances]

    print("Terminating instances with the following IDs:\n %s" % str(ids))
    ec2.instances.filter(InstanceIds=ids).terminate()

def create_instances(worker_count, workers_only):
    if workers_only:
        print("Deploying %d workers" % worker_count)
    else:
        print("Deploying 1 master and %d workers" % worker_count)

    # Launch 1 master instance
    if not workers_only:
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
    worker_ids = [ inst['InstanceId'] for inst in worker_response['Instances'] ]
    ids = worker_ids
    master_id = None
    if not workers_only:
        master_id = master_response['Instances'][0]['InstanceId']
        ids += [ master_id ]

    time.sleep(3)

    print("Waiting for %d instances to start" % len(ids))
    ips = []
    for inst in ec2.instances.filter(InstanceIds=ids):
        print("Waiting for instance to start running")
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

    if workers_only:
        return None
    for inst in ec2.instances.filter(InstanceIds=[master_id]):
        return inst.public_ip_address

def wait_for_ssh(ip):
    attempts = 0
    while(attempts < 200):
        print("Waiting for ssh (%d/200)" % attempts)
        cmd = [ "ssh", "-i", PATH_TO_SSH_KEY, "-o", "StrictHostKeyChecking=no",
                "ec2-user@%s" % ip, "echo 'cerberus'" ]
        result = subprocess.Popen(cmd, stdout=subprocess.PIPE)

        data = result.communicate()
        if b'cerberus' in data[0]:
            return True
        time.sleep(3)

        attempts += 1
    return False

def send_credentials(ips, credentials_path):
    results = []
    run_command("sudo mkdir /home/ec2-user/.aws && sudo chown ec2-user /home/ec2-user/.aws",
                ips, wait=True)
    for ip in ips:
        cmd = [ "scp", "-i", PATH_TO_SSH_KEY, "-o", "StrictHostKeyChecking=no",
                credentials_path, "ec2-user@%s:/home/ec2-user/.aws/credentials" % ip]
        print(cmd)
        results += [ subprocess.Popen(cmd) ]
    for result in results:
        result.wait()

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
if __name__ == '__main__':
    if args.kill:
        kill_instances()

    if args.create != None:
        create_instances(args.create, args.workers_only)

    if args.terminate:
        terminate_containers()

    if args.deploy:
        if args.workers_only and not args.master:
            print("Unable to deploy to workers without a valid master IP. " \
                  "Expected --master to be set")
        else:
            deploy_containers(args.deploy, args.worker_limit, args.master,
                              args.deployment_credentials)


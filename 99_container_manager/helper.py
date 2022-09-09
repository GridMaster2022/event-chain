import boto3
import logging

from config import *

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

sqs_client = boto3.client('sqs')
ecs_client = boto3.client('ecs')


def get_task_count(cluster, family):
    task_count = 0
    response = ecs_client.list_tasks(
        cluster=cluster,
        family=family,
        maxResults=100,
        desiredStatus='RUNNING'
    )
    task_count += len(response['taskArns'])
    while response.get('nextToken'):
        response = ecs_client.list_tasks(
            cluster=cluster,
            family=family,
            maxResults=100,
            desiredStatus='RUNNING',
            nextToken=response['nextToken']
        )
        task_count += len(response['taskArns'])
    return task_count


def update_ecs_desired_count(cluster: str, family: str, task_definition: str, container_limit: int, container_delta: int):
    response = ecs_client.list_tasks(
        cluster=cluster,
        family=family,
        maxResults=100,
        desiredStatus='RUNNING'
    )

    current_container_count = len(response['taskArns'])
    if current_container_count <= container_limit:
        # spawn more containers based on limit and current messages in queue
        if (container_delta + current_container_count) > container_limit:
            containers_to_add = container_limit - current_container_count
        else:
            containers_to_add = container_delta
        if containers_to_add <= 0 and current_container_count == 0:
            containers_to_add = 1

        security_groups = determine_security_group(task_definition)
        subnets = determine_subnets(task_definition)
        if containers_to_add <= 0:
            logging.info('No containers to add for {}'.format(task_definition))
        else:
            logging.info('Adding {} new containers for {}'.format(str(containers_to_add), task_definition))
            # We can only add 10 container per API call, so loop if we need more.
            loop_range, modulus = divmod(containers_to_add, 10)
            for i in range(loop_range):
                response = ecs_client.run_task(
                    cluster=cluster,
                    count=10,
                    enableECSManagedTags=True,
                    enableExecuteCommand=True,
                    taskDefinition=task_definition,
                    launchType='FARGATE',
                    networkConfiguration={
                        'awsvpcConfiguration': {
                            'subnets': subnets,
                            'securityGroups': security_groups,
                            'assignPublicIp': 'DISABLED'
                        }
                    }
                )
            if modulus > 0:
                response = ecs_client.run_task(
                    cluster=cluster,
                    count=modulus,
                    enableECSManagedTags=True,
                    enableExecuteCommand=True,
                    taskDefinition=task_definition,
                    launchType='FARGATE',
                    networkConfiguration={
                        'awsvpcConfiguration': {
                            'subnets': subnets,
                            'securityGroups': security_groups,
                            'assignPublicIp': 'DISABLED'
                        }
                    }
                )


def parse_queue_status(queue_url):
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    queue_length = int(queue_attributes['Attributes']['ApproximateNumberOfMessages'])
    return queue_length, round(queue_length / 2)


def determine_subnets(task_definition):
    if task_definition == 'gridmaster-essim':
        # Prevent spawning of essim in other AZ's then Influx AZ to prevent cross az data transfer charges
        subnets = [
            CONTAINER_SUBNET
        ]
    else:
        subnets = [
            CONTAINER_SUBNET
        ]
    return subnets


def determine_security_group(task_definition):
    if task_definition in ['gridmaster-etm-api', 'gridmaster-init', 'gridmaster-esdl-generator']:
        security_group = GENERAL_CONTAINER_SG
    else:
        security_group = ESSIM_CONTAINER_SG
    return [security_group]

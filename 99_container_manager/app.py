import logging

from helper import update_ecs_desired_count, parse_queue_status, get_task_count
from config import *

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    """ Gridmaster container manager

    Parameters
    ----------
    event: dict, required
        Eventbridge Input Format
        Event doc: https://docs.aws.amazon.com/lambda/latest/dg/with-scheduledevents-example-use-app-spec.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """

    queue_length, queue_delta = parse_queue_status(ESDL_QUEUE_URL)
    task_count = get_task_count('Gridmaster', 'gridmaster-esdl-generator')
    # Check if too many messages in the ETM queue
    if (queue_length > 10 or (task_count == 0 and queue_length > 0)) and task_count < ESDL_CONTAINER_LIMIT:
        logging.info('Current number of messages in esdl queue: {}'.format(queue_length))
        update_ecs_desired_count('Gridmaster', 'gridmaster-esdl-generator', 'gridmaster-esdl-generator',
                                 ESDL_CONTAINER_LIMIT, queue_delta)
    elif task_count == INIT_CONTAINER_LIMIT:
        logging.info('Container limit reached for esdl generator')

    queue_length, queue_delta = parse_queue_status(INIT_QUEUE_URL)
    task_count = get_task_count('Gridmaster', 'gridmaster-init')
    # Check if too many messages in the ETM queue
    if (queue_length > 10 or (task_count == 0 and queue_length > 0)) and task_count < INIT_CONTAINER_LIMIT:
        logging.info('Current number of messages in init queue: {}'.format(queue_length))
        update_ecs_desired_count('Gridmaster', 'gridmaster-init', 'gridmaster-init', INIT_CONTAINER_LIMIT,
                                 queue_delta)
    elif task_count == INIT_CONTAINER_LIMIT:
        logging.info('Container limit reached for init')

    queue_length, queue_delta = parse_queue_status(ETM_QUEUE_URL)
    task_count = get_task_count('Gridmaster', 'gridmaster-etm-api')
    # Check if too many messages in the ETM queue
    if (queue_length > 10 or (task_count == 0 and queue_length > 0)) and task_count < ETM_CONTAINER_LIMIT:
        logging.info('Current number of messages in ETM queue: {}'.format(queue_length))
        update_ecs_desired_count('Gridmaster', 'gridmaster-etm-api', 'gridmaster-etm-api', ETM_CONTAINER_LIMIT,
                                 queue_delta)
    elif task_count == ETM_CONTAINER_LIMIT:
        logging.info('Container limit reached for ETM')

    # Get current queued messages in ESSIM queue
    queue_length, queue_delta = parse_queue_status(ESSIM_QUEUE_URL)
    task_count = get_task_count('Gridmaster', 'gridmaster-essim')
    # Check if too many messages in the ESSIM queue
    if (queue_length > 10 or (task_count == 0 and queue_length > 0)) and task_count < ESSIM_CONTAINER_LIMIT:
        logging.info('Current number of messages in ESSIM queue: {}'.format(queue_length))
        update_ecs_desired_count('Gridmaster', 'gridmaster-essim', 'gridmaster-essim', ESSIM_CONTAINER_LIMIT,
                                 queue_delta)
    elif task_count == ESSIM_CONTAINER_LIMIT:
        logging.info('Container limit reached for ESSIM')

    queue_length, queue_delta = parse_queue_status(LOADFLOW_QUEUE_URL)
    task_count = get_task_count('Gridmaster', 'gridmaster-gasunie-loadflow')
    # Check if too many messages in the Load flow queue
    if (queue_length > 10 or (task_count == 0 and queue_length > 0)) and task_count < LOADFLOW_CONTAINER_LIMIT:
        logging.info('Current number of messages in Gasunie queue: {}'.format(queue_length))
        update_ecs_desired_count('Gridmaster', 'gridmaster-gasunie-loadflow', 'gridmaster-gasunie-loadflow',
                                 LOADFLOW_CONTAINER_LIMIT, queue_delta)
    elif task_count == LOADFLOW_CONTAINER_LIMIT:
        logging.info('Container limit reached for Loadflow')

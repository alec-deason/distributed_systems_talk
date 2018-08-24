from hierachical_aggregator.message_broker import MessageSchema, RedisMessageQueue
from hierachical_aggregator.hold import Semaphore

JOB_SCHEMA = MessageSchema({"node_name": str, "parent_node_name": str})


def create_job(node_name, parent_node_name, children, host, port):
    sem = Semaphore(f"{node_name}_lock", host, port)
    sem.add_dependencies(*children)
    q = RedisMessageQueue(JOB_SCHEMA, "job_queue", host, port)
    q.send({"node_name": node_name, "parent_node_name": parent_node_name})


def get_job(host, port):
    q = RedisMessageQueue(JOB_SCHEMA, "job_queue", host, port)
    while True:
        message = q.recieve(block=False)
        if message is None:
            return None
        sem = Semaphore(message["node_name"], host, port)
        if sem.is_clear():
            return message
        else:
            q.send(message)


def finish_job(node_name, parent_node_name, host, port):
    sem = Semaphore(parent_node_name, host, port)
    sem.clear_dependencies(node_name)


def give_up_job(node_name, parent_node_name, host, port):
    q = RedisMessageQueue(JOB_SCHEMA, "job_queue", host, port)
    q.send({"node_name": node_name, "parent_node_name": parent_node_name})


def jobs_remaining(host, port):
    q = RedisMessageQueue(JOB_SCHEMA, "job_queue", host, port)
    return q.queue_length()

import redis
import uuid
import json


class MessageError(Exception):
    pass


def _validate_message(schema, message):
    checked = set()
    for k, v in message.items():
        if k in schema:
            if isinstance(schema[k], dict):
                _validate_message(schema[k], v)
                checked.add(k)
            elif not isinstance(v, schema[k]):
                raise MessageError("Message does not match schema")
            else:
                checked.add(k)
        else:
            raise MessageError("Message does not match schema")
    if checked != set(schema.keys()):
        raise MessageError("Message does not match schema")


class MessageSchema:
    def __init__(self, schema):
        self._schema = schema

    def validate_message(self, message):
        _validate_message(self._schema, message)


class MessageQueue:
    def __init__(self, schema, name):
        self._schema = schema
        self._name = name

    def send(self, message):
        self._schema.validate_message(message)
        return self._send(message)

    def _send(self, message):
        raise NotImplementedError("Subclass must provide _send method")

    def recieve(self, block=True):
        message = self._recieve(block)
        if message is not None:
            self._schema.validate_message(message)
        return message

    def _recieve(self, block):
        raise NotImplementedError("Subclass must provide _recieve method")

    def queue_length(self):
        return self._queue_length()

    def _queue_length(self):
        raise NotImplementedError("Subclass must provide _queue_length method")


class RedisMessageQueue(MessageQueue):
    def __init__(self, schema, name, host, port):
        super().__init__(schema, name)
        self._redis = redis.Redis(host=host, port=port)

    def _send(self, message):
        message_id = str(uuid.uuid4())
        message_blob = json.dumps(message)

        with self._redis.pipeline() as p:
            p.lpush(self._name, message_id)
            p.set(message_id, message_blob)
            p.execute()

        return message_id

    def _recieve(self, block):
        if block:
            message = self._redis.brpop(self._name)
        else:
            message = self._redis.rpop(self._name)

        if message is not None:
            message = self._redis.get(message)
            message = json.loads(message)

        return message

    def _queue_length(self):
        return self._redis.llen(self._name)

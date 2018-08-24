import argparse
import time

import redis


def listen(in_connection, out_connection, source_node, destination_node):
    pubsub = in_connection.pubsub()
    pubsub.subscribe(source_node, "flush")

    current_sum = 0
    while True:
        message = pubsub.get_message()
        if message and message["type"] == "message":
            if message["channel"].decode() == "flush":
                out_connection.set(source_node, current_sum)
            else:
                print("Message", message)
                if destination_node != "None":
                    out_connection.publish(destination_node, message["data"])
                current_sum += float(message["data"])
        else:
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port")
    parser.add_argument("source_node")
    parser.add_argument("destination_node")
    args = parser.parse_args()

    in_connection = redis.StrictRedis(host=args.host, port=args.port)
    out_connection = redis.StrictRedis(host=args.host, port=args.port)
    listen(in_connection, out_connection, args.source_node, args.destination_node)


if __name__ == "__main__":
    main()

import ast
import binascii
from json import loads
import logging
import grpc
import threading
from concurrent import futures

from kafka import KafkaConsumer
# from consumer.consumer import kafka_consumer_thread
from multiprocessing import Process
from proto import channel_pb2, channel_pb2_grpc
from confluent_kafka import Consumer
# import channel_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.empty_pb2 import Empty  # Import the Empty message
import vars

kafka_topics = ['subsCount']
bootstrap_servers = [f'''{vars.KAFKA_BROKER_URL}:{vars.KAFKA_BROKER_PORT}''']
# consumer = Consumer({'bootstrap.servers': f'''{vars.KAFKA_BROKER_URL}:{vars.KAFKA_BROKER_PORT}''', 'group.id': 'channel_service', 'auto.offset.reset': 'earliest'})

# consumer.subscribe(topics=['subsCount'])

class ChannelServiceServicer(channel_pb2_grpc.channelServiceServicer):
    def getPostStat(self, request, context):
        # Implement your getPostStat logic here
        response = channel_pb2.PostStatResponse()
        response.channel_id = request.channel_id
        response.post_id = request.post_id
        response.views = 100  # Replace with actual data
        response.shares = 50  # Replace with actual data
        return response

    def getChannelInfo(self, request, context):
        # Implement your getChannelInfo logic here
        response = channel_pb2.ChannelInfoResponse()
        for channel_id in request.channel_id:
            channel_info = response.channel_info.add()
            channel_info.channel_id = channel_id
            channel_info.name = "Channel Name"  # Replace with actual data
            channel_info.link = "https://example.com/channel"  # Replace with actual data
            channel_info.description = "Channel Description"  # Replace with actual data
            channel_info.subscribers = 10000  # Replace with actual data
        return response

    def getChannels(self, request, context):
        # Implement your getChannels logic here
        for channel_id in range(1, 6):  # Example: Return channels with IDs 1 to 5
            response = channel_pb2.GetChannelsResponse()
            response.channel_ids.extend([channel_id])
            yield response

    def getChannelSubsHistory(self, request, context):
        # Implement your getChannelSubsHistory logic here
        for channel_id in request.channel_id:
            response = channel_pb2.ChannelSubsHistoryResponse()
            response.channel_subs_history.extend([
                channel_pb2.ChannelSubsHistory(
                    channel_id=channel_id,
                    moment=Timestamp(seconds=1609459200),  # Replace with actual data
                    subs=1000,  # Replace with actual data
                ),
            ])
            yield response

    def getPostStatHistory(self, request, context):
        # Implement your getPostStatHistory logic here
        for channel_id in request.channel_id:
            for history_type in request.history_type:
                response = channel_pb2.PostStatHistoryResponse()
                response.post_stat_history.extend([
                    channel_pb2.PostStatHistory(
                        channel_id=channel_id,
                        post_history=[
                            channel_pb2.PostHistoryType(
                                history_type=history_type,
                                moment=Timestamp(seconds=1609459200),  # Replace with actual data
                                count=100,  # Replace with actual data
                            ),
                        ],
                    ),
                ])
                yield response

    def getPosts(self, request, context):
        # Implement your getPosts logic here
        for channel_id in request.channel_ids:
            response = channel_pb2.GetPostsResponse()
            response.channels_posts.extend([
                channel_pb2.ChannelPosts(
                    channel_id=channel_id,
                    post_id=[1, 2, 3],  # Replace with actual data
                ),
            ])
            yield response

def create_consumer(topic):
    try:
        consumer = Consumer({"bootstrap.servers": 'localhost:29092',
                             "group.id": vars.KAFKA_CONSUMER_GROUP,
                            #  "client.id": socket.gethostname(),
                            #  "isolation.level": "read_committed",
                            #  "default.topic.config": {"auto.offset.reset": "latest", # Only consume new messages
                            #                           "enable.auto.commit": False}
                             })
        consumer.subscribe([topic])
    except Exception as e:
        logging.exception("Couldn't create the consumer")
        consumer = None

    return consumer


def consume_data():
    consumer = KafkaConsumer("subsCount", bootstrap_servers='localhost:29092', auto_offset_reset='earliest', enable_auto_commit=False)
    while True:
        # message = consumer.poll(timeout=5)
        for message in consumer:
            print(message.value.decode())
        
        # print("working")
        # if message is None:
        #     continue
        # elif message.error():
        #     logging.error("Consumer error: {}".format(message.error()))
        #     continue
        # else:
        #     print(3)
        #     print(f'''Message: {message.value.decode()}''')
        
        



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    channel_pb2_grpc.add_channelServiceServicer_to_server(ChannelServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    
    t1 = Process(target=consume_data)
    t1.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Start the gRPC server
    serve()
    # pass
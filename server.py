import ast
import binascii
import datetime
from json import JSONDecodeError, loads
import json
import logging
import random
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
from database.database import Database
from aggregate import Aggregator




kafka_topics = ['subsCount']
bootstrap_servers = [f'''{vars.KAFKA_BROKER_URL}:{vars.KAFKA_BROKER_PORT}''']

class ChannelServiceServicer(channel_pb2_grpc.channelServiceServicer):
    def getPostStat(self, request, context):
        print("getPostStat")
        # Works
        # Implement your getPostStat logic here
        db_instance = Aggregator()
        post_stats = db_instance.postStats(request.channel_id, request.post_id)
        response = channel_pb2.PostStatResponse()
        response.channel_id = request.channel_id
        response.post_id = request.post_id
        response.views = post_stats["views"]
        response.shares = post_stats["shares"]
        return response

    def getChannelInfo(self, request, context):
        # Works
        print("getChannelInfo")
        # Implement your getChannelInfo logic here
        db_instance = Aggregator()
        channels_info = db_instance.channelInfo(request.channel_id)
        response = channel_pb2.ChannelInfoResponse()
        for channel in channels_info:
            channel_info = response.channel_info.add()
            channel_info.channel_id = channel["channel_id"]
            channel_info.name = channel["channel_name"]  # Replace with actual data
            channel_info.link = channel["channel_title"]  # Replace with actual data
            channel_info.description = channel["channel_description"]  # Replace with actual data
            channel_info.subscribers = channel["subs"]  # Replace with actual data
        return response

    def getChannels(self, request, context):
        # Works
        db_instance = Aggregator()
        channels = db_instance.channels()
        
        for channel_id in channels:
            res = channel_pb2.GetChannelsResponse(channel_id=channel_id)
            yield res

    def getChannelSubsHistory(self, request, context):
        # Works
        print("getChannelSubsHistory")
        response = channel_pb2.ChannelSubsHistoryResponse()
        db_instance = Aggregator()
        
        for channel_id in request.channel_id:
            channel_subs_history = channel_pb2.ChannelSubsHistory()
            channel_subs_history.channel_id = channel_id
            subs = db_instance.channelSubsHistory(channel_id)
            for measurement in subs:  # You can adjust the number of random data points
                history_values = channel_pb2.HistoryValues()
                timestamp = Timestamp()
                timestamp.FromDatetime(datetime.datetime.fromtimestamp(measurement["moment"]))
                history_values.moment.CopyFrom(timestamp)
                history_values.value = measurement["subs_count"]  # Generate a random value
                channel_subs_history.history_values.append(history_values)
            response.channel_subs_history.append(channel_subs_history)

        return response
        # return response

    def getPostStatHistory(self, request, context):
        # Works
        print("getPostStatHistory")

        db_instance = Aggregator()
        # post_stats = db_instance.postStats(request.channel_id, request.post_id, request.history_type)
        response = channel_pb2.PostStatHistoryResponse()

        
        post_stat_history =channel_pb2.PostStatHistory()

        # Generate random channel ID and post ID
        post_stat_history.channel_id = request.channel_id
        post_stat_history.post_id = request.post_id

        for history_type in request.history_type:
            post_history = channel_pb2.PostHistory()
            post_history.history_type = history_type
            values = db_instance.postStatHistory(request.channel_id, request.post_id, history_type)
            for value in values:  # You can adjust the number of random data points
                history_values = channel_pb2.HistoryValues()
                timestamp = Timestamp()
                timestamp.FromDatetime(datetime.datetime.fromtimestamp(value["moment"]))
                history_values.moment.CopyFrom(timestamp)
                history_values.value = value["value"]  # Generate a random value
                post_history.history_values.append(history_values)
            post_stat_history.post_history.append(post_history)
        response.post_stat_history.append(post_stat_history)
        return response

    def getPosts(self, request, context):
        # Works
        db_instance = Aggregator()
        print("getPosts")
        moment = request.moment
        moment = datetime.datetime.fromtimestamp(request.moment.seconds + request.moment.nanos / 1e9)
        response = channel_pb2.GetPostsResponse()
        for channel_id in request.channel_ids:
            channel_posts = channel_pb2.ChannelPosts()
            channel_posts.channel_id = channel_id
            posts = db_instance.posts(channel_id, int(moment.timestamp()))
            for id in posts:  # You can adjust the number of random post IDs
                channel_posts.post_id.append(id)  # Generate a random post ID
            response.channels_posts.append(channel_posts)
        return response
    
    def getPostInfo(self, request, context):
        response = channel_pb2.GetPostInfoResponse()
        db_instance = Aggregator()
        for post_id in request.post_ids:
            post_info = channel_pb2.PostInfo()
            post_info.post_id = post_id  # Generate a random post ID
            post_info_dict = db_instance.postInfo(request.channel_id, post_id)
            # Generate random content (assuming a string field)
            post_info.content.value = post_info_dict.get("content", "NO CONTENT AVAILABLE")

            # Generate random views and shares (assuming int64 fields)
            post_info.views = post_info_dict.get("views", 0)
            post_info.shares = post_info_dict.get("shares", 0)

            response.post_info.append(post_info)

        return response
        


# def create_consumer(topic):
#     try:
#         consumer = Consumer({"bootstrap.servers": 'localhost:29092',
#                              "group.id": vars.KAFKA_CONSUMER_GROUP,
#                             #  "client.id": socket.gethostname(),
#                             #  "isolation.level": "read_committed",
#                             #  "default.topic.config": {"auto.offset.reset": "latest", # Only consume new messages
#                             #                           "enable.auto.commit": False}
#                              })
#         consumer.subscribe([topic])
#     except Exception as e:
#         logging.exception("Couldn't create the consumer")
#         consumer = None

#     return consumer


def consume_data(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:29092', auto_offset_reset='latest', enable_auto_commit=False)
    while True:
        # message = consumer.poll(timeout=5)
        for message in consumer:
            message_value = message.value.decode()
            print(message.value.decode())
            try:
                message_data = json.loads(message_value)
                db_instance = Database()
                db_instance.addMessage(topic, message_data)
            except JSONDecodeError as json_error:
                print(f"Error decoding JSON: {json_error}")
            
        
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    channel_pb2_grpc.add_channelServiceServicer_to_server(ChannelServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    
    for topic in ["subsCount", "channelMeta", "postContent", "postStat"]:
        t1 = Process(target=consume_data, args=(topic,))
        t1.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Start the gRPC server
    serve()
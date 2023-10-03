import grpc
import threading
from concurrent import futures
from consumer.consumer import kafka_consumer_thread
from proto import channel_pb2, channel_pb2_grpc
# import channel_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.empty_pb2 import Empty  # Import the Empty message

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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    channel_pb2_grpc.add_channelServiceServicer_to_server(ChannelServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    # Define Kafka topics to consume
    kafka_topics = ['topic1', 'topic2', 'topic3']

    # Start Kafka consumer threads for each topic
    for topic in kafka_topics:
        consumer_thread = threading.Thread(target=kafka_consumer_thread, args=(topic,))
        consumer_thread.daemon = True  # This allows the threads to exit when the main program exits
        consumer_thread.start()

    # Start the gRPC server
    serve()
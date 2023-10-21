from database.database import Database

class Aggregator:
    def __init__(self):
        pass

    def channelInfo(self,channel_id) -> dict:
        pass


    def channels(self) -> list:
        db_instance = Database()
        messages = db_instance.getMessages('channelMeta')
        channels = set()
        for message in messages:
            channels.add(message["channel_id"])
        return list(channels)


    def postsStats(self,channel_id, post_id) -> dict:
        pass

    def channelSubsHistory(self,channel_id) -> dict:
        pass

    def postStatHistory(self,channel_id, post_id) -> dict:
        pass

    def posts(self,channel_id, moment) -> dict:
        pass

    def postInfo(self,channel_id, post_id) -> dict:
        pass

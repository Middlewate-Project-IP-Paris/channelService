from database.database import Database

class Aggregator:
    def __init__(self):
        pass

    def getSubs(self, channel_id) -> int:
        db_instance = Database()
        messages_raw = db_instance.getMessages('subscount')
        filtered_data = [item for item in messages_raw if item["channel_id"] == channel_id]
        subs = max(filtered_data, key=lambda x: x["moment"])["subs_count"]
        return subs
    
    def channelInfo(self,channel_ids) -> list:
        db_instance = Database()
        messages_raw = db_instance.getMessages('channelMeta')
        channels_info = []
        for message in messages_raw:
            if message["channel_id"] in channel_ids:
                message["subs"] = self.getSubs(message["channel_id"])
                channels_info.append(message)


        return channels_info


    def channels(self) -> list:
        db_instance = Database()
        messages = db_instance.getMessages('channelMeta')
        channels = set()
        for message in messages:
            channels.add(message["channel_id"])
        return list(channels)


    def postStats(self,channel_id, post_id) -> dict:
        db_instance = Database()
        messages_raw = db_instance.getMessages('postStat')
        filtered_data = [item for item in messages_raw if item["channel_id"] == channel_id and item["post_id"] == post_id]
        latest_info = max(filtered_data, key=lambda x: x["moment"])
        return latest_info

    def channelSubsHistory(self,channel_id) -> dict:
        db_instance = Database()
        messages_raw = db_instance.getMessages('subscount')
        filtered_data = [item for item in messages_raw if item["channel_id"] == channel_id]
        return filtered_data

    def postStatHistory(self,channel_id, post_id) -> dict:
        pass

    def posts(self,channel_id, moment) -> dict:
        pass

    def postInfo(self,channel_id, post_id) -> dict:
        pass

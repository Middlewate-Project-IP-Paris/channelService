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

    def postStatHistory(self,channel_id, post_id, history_type) -> dict:
        posts_stats_history = []
        db_instance = Database()
        messages_raw = db_instance.getMessages('postStat')
        filtered_data = [item for item in messages_raw if item["channel_id"] == channel_id and item["post_id"] == post_id]
        for post in filtered_data:
            cur_post = {
                "moment" : post["moment"]
            }
            if history_type == 2:
                cur_post["value"] = post["shares"]
            if history_type == 1:
                cur_post["value"] = post["views"]
            posts_stats_history.append(cur_post)
        return posts_stats_history

    def posts(self,channel_id, moment) -> dict:
        db_instance = Database()
        messages_raw = db_instance.getMessages("postStat")
        filtered_data = [item["post_id"] for item in messages_raw if item["channel_id"] == channel_id and item["publicated_at"] >= moment]
        return filtered_data
        # pass

    def postInfo(self,channel_id, post_id) -> dict:
        post_info = {}
        db_instance = Database()
        content = db_instance.getMessages('postContent')
        post_content = [item for item in content if item["channel_id"] == channel_id and item["post_id"] == post_id]
        if post_content != []:
            post_info["post_id"] = post_id
            post_info["content"] = post_content[0]["content"]
            
        
        messages_raw = db_instance.getMessages('postStat')
        filtered_data = [item for item in messages_raw if item["channel_id"] == channel_id and item["post_id"] == post_id]
        
        if filtered_data != []:
            latest_info = max(filtered_data, key=lambda x: x["moment"])
            post_info["views"] = latest_info["views"]
            post_info["shares"] = latest_info["shares"]

        return post_info
        # pass


# db_instance = Aggregator()
# print(db_instance.postInfo(20, 20))
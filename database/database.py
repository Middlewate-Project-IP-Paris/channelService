import json


class Database:
    def __init__(self):
        pass

    def addMessage(self, topic, input_message):
        with open(f'''database/{topic}.json''', 'r') as file:
            messages_raw =  json.load(file)
        messages_list = list(messages_raw['messages'])
        messages_list.append(input_message)
        json_dict = {"messages": messages_list}
        json_object = json.dumps(json_dict, indent=4)
        with open(f'''database/{topic}.json''', "w") as outfile:
            outfile.write(json_object)

    def getMessages(self, topic) -> list:
        with open(f'''database/{topic}.json''', 'r') as file:
            messages =  json.load(file)['messages']
        return messages





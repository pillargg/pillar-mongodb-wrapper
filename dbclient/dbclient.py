import datetime
import math
import os

from pymongo import MongoClient
from bson import ObjectId

from .lib import duration_to_int


class DBClient:
    """
    Class that handles all connections to and from the MongoDB database.
    """

    def __init__(self, output_file_location='', override_url=None):
        """
        Initializes the connection to the MongoDB database
        """

        self.db_name = os.environ.get("MONGODB_DBNAME")
        print('DB NAME: ' + os.environ.get("MONGODB_DBNAME"))

        self.output_file_location = output_file_location

        self.mongo_client = None

        if override_url is None:
            self.mongo_client = MongoClient(
                'mongodb+srv://admin:' +
                os.environ.get('MONGODB_PASS') +
                '@c0.relki.mongodb.net')
        else:
            self.mongo_client = MongoClient(override_url)

        self.db = self.mongo_client[self.db_name]
        self.messages_collection = self.db["messages"]
        self.streams_collection = self.db["streams"]
        self.clip_collection = self.db["clips"]

    def input_message(self, username, contents, timestamp, streamer, video=None, platform="twitch"):
        """
        Inputs a message into the database
        username and streamer are strings without spaces
        contents is a string
        timestamp is a datetime

        returns the ID of the object inserted
        """

        message_document = {
            "username": username,
            "contents": contents,
            "timestamp": str(timestamp),
            "author": streamer,
            "platform_video_id": video,
            "platform": platform
        }

        return self.messages_collection.insert_one(message_document).inserted_id

    def input_stream(self, author, timestamp, viewers, duration, video_id=None, s3_object=None, platform="twitch"):
        """
        Inputs a stream into the database
        author is a string without spaces
        viewers is an int
        timestamp is a datetime
        duration is either a string or an integer
        if it is a string it will be converted to an integer.
        the string must be of the format "XXhXXmXXs" where XX represent integers
        e.g. ("captainSparklez", datetime.datetime.now(), 500)

        returns the ID of the object inserted
        """

        if type(duration) == str:
            duration = duration_to_int(duration)

        stream_document = {
            "author": author,
            "started_at": str(timestamp),
            "viewers": viewers,
            "duration": duration,
            "platform_video_id": video_id,
            "s3_object_name": s3_object,  # this should be the platform id
            "platform": platform  # for now assume this is twitch
        }

        return self.streams_collection.insert_one(stream_document).inserted_id

    def input_clip(self, clip_data):
        '''
        Inputs a clip to the database. Here is an example of what the clip data should look like.
        ```JSON
        {
            "title": "Chandler gets headshot",
            "description": "I get shot in the face",
            "clip_started_at": "2021-02-22T18:51:52Z",
            "author": "chand1012",
            "type": "mp4",
            "original_video": {
                "platform_video_id": "vPOOO1w_Oko",
                "video_db_id": "603530fa96a86dc591e8a044",
                "source": "youtube",
                "started_at": "2021-02-22T18:51:52Z",
                "duration": 3600
            },
            "position": 12,
            "duration": 60,
            "tags": [],
            "s3_url": "",
            "score": 69
        }
        ```
        '''

        return self.clip_collection.insert_one(clip_data).inserted_id

    def update_clip_url(self, clip_id, url):
        '''
        Updates clip by mongodb ID. Returns the new value.
        '''

        query = {'_id': ObjectId(clip_id)}
        values = {'$set': {'s3_url': url}}

        self.clip_collection.update_one(query, values)

        return self.clip_collection.find_one(query)

    def get_clips(self, platform_video_id, author=None, platform='twitch'):
        '''
        Gets clips based on their platform_video_id, author, and platform.
        '''

        query = {
            'platform_video_id': platform_video_id,
            'platform': platform
        }

        if author:
            query['author'] = author

        return self.clip_collection.find(query)

    def get_chat_messages(self, platform_video_id, author=None, platform='twitch'):
        '''
        Gets the chat messages for the given platform_video_id and author
        '''

        query = {
            'platform_video_id': platform_video_id,
            'platform': platform
        }

        if author:
            query['author'] = author

        return self.messages_collection.find(query)

    def get_stream_by_mongo_id(self, _id):
        '''
        Gets the stream with the given MongoDB ID
        '''

        return self.streams_collection.find_one({'_id': ObjectId(_id)})

    def get_latest_stream_from_author(self, author):
        '''
        Gets the latest stream from an author
        '''
        return self.streams_collection.find_one({'author': author}, sort=[
            ('_id', -1)])  # sort in descending order

    def get_chat_messages_in_date_range(self, platform_video_id, start, end, platform='twitch', author=None):
        '''
        Gets all chat messages in a date range from `start` to `end`, where `start` and `end` are arrow date types.
        `start` and `end` can also be datestrings in the following format: `%Y-%m-%dT%H:%M:%S.%f+00:00`
        '''

        query = {
            'platform_video_id': platform_video_id,
            'platform': platform,
            'timestamp': {
                '$gte': str(start),
                '$lt': str(end)
            }
        }

        if author:
            query['author'] = author

        return self.messages_collection.find(query)

    def purge_clips(self, platform_video_id, author=None, platform='twitch'):
        '''
        Deletes clips with the matching inputs. Returns the number of deleted clips.
        '''

        query = {
            'platform': platform,
            'platform_video_id': platform_video_id
        }

        if author:
            query['author'] = author

        return self.clip_collection.delete_many(query).deleted_count

    def purge_messages(self, platform_video_id, author=None, platform='twitch'):
        '''
        Deletes messages with the matching inputs. Returns the number of deleted messages.
        '''
        query = {
            'platform': platform,
            'platform_video_id': platform_video_id
        }

        if author:
            query['author'] = author

        return self.messages_collection.delete_many(query).deleted_count

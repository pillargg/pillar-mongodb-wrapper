import datetime
import math
import os

from pymongo import MongoClient

from .lib import duration_to_int


class DBClient:
    """
    Class handles all connections to and from the MongoDB database.
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
        self.messagesCollection = self.db["messages"]
        self.streamsCollection = self.db["streams"]

    def input_message(self, username, contents, thedatetime, streamer, video=None):
        """
        Inputs a message into the database
        username and streamer are strings without spaces
        contents is a string
        thedatetime is a datetime

        returns the ID of the object inserted
        """

        messageDocument = {
            "username": username,
            "contents": contents,
            "datetime": str(thedatetime),
            "streamer": streamer,
            "video_id": video}

        return self.messagesCollection.insert_one(messageDocument).inserted_id

    def input_stream(self, streamer, thedatetime, numviewers, duration):
        """
        Inputs a stream into the database
        streamer is a string without spaces
        numviewers is an int
        thedatetime is a datetime
        duration is either a string or an integer
        if it is a string it will be converted to an integer.
        the string must be of the format "XXhXXmXXs" where XX represent integers
        e.g. ("captainSparklez", datetime.datetime.now(), 500)

        returns the ID of the object inserted
        """

        if type(duration) == str:
            duration = duration_to_int(duration)

        streamsDocument = {"streamer": streamer, "datetime": str(
            thedatetime), "numviewers": str(numviewers), "duration": duration}

        return self.streamsCollection.insert_one(streamsDocument).inserted_id

    def analyze_number_of_stream_viewers(self, streamer, datetime, _id=None):
        """
        This function returns a streamers viewers over time
        This should be moved to the 
        """
        stream = None
        if not _id:
            stream = self.streamsCollection.find_one({'streamer': streamer}, sort=[
                ('_id', -1)])  # sort in descending order
        else:
            stream = self.streamsCollection.find_one({'_id': _id})

        # read length
        duration_in_seconds = stream.get('duration')
        # get start time and convert to datetime
        clip_start_time = datetime.datetime.strptime(
            stream.get('datetime'), '%Y-%m-%d %H:%M:%S.%f')

        clip_duration = datetime.timedelta(seconds=duration_in_seconds)

        clip_end_time = clip_start_time + clip_duration

        results = self.streamsCollection.find({"streamer": streamer, "datetime": {
                                              '$gte': str(clip_start_time), '$lt': str(clip_end_time)}})

        total_data = []
        for result in results:
            clips_real_time = datetime.datetime.strptime(
                result['datetime'], '%Y-%m-%d %H:%M:%S.%f')
            output_time = clips_real_time - clip_start_time
            total_data.append({'deltatime_from_start_of_clip': output_time,
                               'num_viewers': result['numviewers'], 'source_clip': streamer})

        return total_data

    def clip_message_analytics(self, streamer, _id=None):
        """
        This function returns a streamers message information during the duration of a videoclip

        This should be moved to the clip_processor project by extending this class
        """

        stream = None

        if not _id:
            stream = self.streamsCollection.find_one({'streamer': streamer}, sort=[
                ('_id', -1)])  # sort in descending order
        else:
            stream = self.streamsCollection.find_one({'_id': _id})

        stream = self.streamsCollection.find_one({'streamer': streamer}, sort=[
                                                 ('_id', -1)])  # sort in descending order

        # read length
        duration_in_seconds = stream.get('duration')
        # get start time and convert to datetime
        clip_start_time = datetime.datetime.strptime(
            stream.get('datetime'), '%Y-%m-%d %H:%M:%S.%f')

        minutes = math.ceil(duration_in_seconds / 60)

        # get average messages/min
        # repeated searches through mongo db using rotating minutemark

        # make array of datetimes

        total_data = []

        start_time = clip_start_time - datetime.timedelta(minutes=1)
        for x in range(minutes):
            # increment start time
            start_time = start_time + datetime.timedelta(minutes=1)
            end_time = start_time + datetime.timedelta(minutes=1)
            results = self.messagesCollection.find({"streamer": streamer, "datetime": {
                                                   '$gte': str(start_time), '$lt': str(end_time)}})

            if results.count():

                output_start_time = start_time - clip_start_time
                output_end_time = end_time - clip_start_time
                print("Adding data for time: " +
                      str(output_start_time) + ' to ' + str(output_end_time))

                total_data.append({'start_time': output_start_time,
                                   'end_time': output_end_time,
                                   'messeges_count': results.count(),
                                   'source_clip': streamer})

        results = self.messagesCollection.find({"streamer": streamer})

        print("total messages: " + str(results.count()))

        return total_data

import datetime
import math
import os

from pymongo import MongoClient

from .lib import duration_to_int


class DBClient:
    """
    Class handles all connections to and from the MongoDB database.
    """

    def __init__(self, output_file_location=''):
        """
        Initializes the connection to the MongoDB database
        """
        print('DB NAME: ' + os.environ.get("MONGODB_DBNAME"))
        self.db_name = os.environ.get("MONGODB_DBNAME")

        self.output_file_location = output_file_location

        self.mongo_client = MongoClient(
            'mongodb+srv://admin:' +
            os.environ.get('MONGODB_PASS') +
            '@c0.relki.mongodb.net')
        self.db = self.mongo_client[self.db_name]
        self.messagesCollection = self.db["messages"]
        self.streamsCollection = self.db["streams"]

    def input_message(self, username, contents, thedatetime, streamer):
        """
        Inputs a message into the database
        username and streamer are strings without spaces
        contents is a string
        thedatetime is a datetime
        """

        messageDocument = {
            "username": username,
            "contents": contents,
            "datetime": str(thedatetime),
            "streamer": streamer}
        self.messagesCollection.insert_one(messageDocument)

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
        """

        if type(duration) == str:
            duration = duration_to_int(duration)

        streamsDocument = {"streamer": streamer, "datetime": str(
            thedatetime), "numviewers": str(numviewers), "duration": duration}

        self.streamsCollection.insert_one(streamsDocument)

    def recreate_db(self):
        """
        drops the database and recreates it from scratch
        WARNING: all data will be lost
        """

        self.mongo_client.drop_database(self.db_name)
        db = self.mongo_client[self.db_name]
        messagesCollection = db["messages"]
        streamsCollection = db["streams"]

        messageDocument = {
            "username": "testUser",
            "contents": "I am posting a new message",
            "datetime": str(
                datetime.datetime.now()),
            "streamer": "teststreamer"}
        streamsDocument = {"streamer": "teststreamer", "datetime": str(
            datetime.datetime.now()), "numviewers": "9002"}

        messagesCollection.insert_one(messageDocument)
        streamsCollection.insert_one(streamsDocument)

        print("Database Recreated")

    def analyze_number_of_stream_viewers(self, streamer, datetime):
        """
        This function returns a streamers viewers over time
        """

        stream = self.streamsCollection.find_one({'streamer': streamer}, sort=[
                                                 ('_id', -1)])  # sort in descending order

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

    def analyze_stream(self, streamer):
        """
        This function returns a streamers message information during the duration of a videoclip
        """

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

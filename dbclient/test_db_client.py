from lib import db_connect


def test_db_init():
    output_file_location = './'
    assert db_connect.DBClient(output_file_location)

# def test_analyze_number_of_stream_viewers():
#     #creating video file that has length
#     #inserting test messages into db
#     #run analysis and see if we get expected results

#     #-> when no messages were sent over period of time
#     #-> the same number of messages were sent over a period of time
#     #-> a varying number of messages were sent over time.

#     assert db_connect.analyze_number_of_stream_viewers()

# def test_input_message():
#     dbclient = DBClient(mockmongo)
#     dbclient.inputMessage(username, contents, thedatetime, streamer)

import re


def duration_to_int(duration):
    '''
    Converts a Twitch Duration String (see [here](https://dev.twitch.tv/docs/api/reference#get-videos)) to the duration in seconds as an integer.
    '''
    hours = re.findall(r'\d+[h]', duration)
    minutes = re.findall(r'\d+[m]', duration)
    seconds = re.findall(r'\d+[s]', duration)
    number = 0
    if hours:
        number += int(hours[0].replace('h', '')) * 3600

    if minutes:
        number += int(minutes[0].replace('m', '')) * 60

    if seconds:
        number += int(seconds[0].replace('s', ''))

    return number

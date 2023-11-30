import re
import datetime

def parse_times(query):
    """Find the "earliest" and "latest" parameter's values from the user's query, if they supplied them

    Keyword arguments:
    query -- the Splunk query supplied by the user

    Returns:
    earliest value -- the value of the "earliest" parameter
    latest_value -- the value of the "latest" parameter
    """

    earliest_value = None
    latest_value = None
    
    earliest_pattern = re.search(r"earliest ?= ?[\"\']?([^\s\'\"]+)[\s\"\']", query)
    if earliest_pattern:
        earliest_value = earliest_pattern.group(1)
    
    latest_pattern = re.search(r"latest ?= ?[\"\']?([^\s\'\"]+)[\s\"\']", query)
    if latest_pattern:
        latest_value = latest_pattern.group(1)
    
    return earliest_value, latest_value

def splunk_time(intime):
    """ Converts Splunk time to the required time format for the Splunk API

    Keyword arguments:
    intime -- no idea

    Returns:
    outtime -- no idea what this is either
    """
    
    m = re.search("\d{1,2}\/\d{1,2}\/\d{4}", intime)

    if m:
        tmp_dt = datetime.datetime.strptime(intime, "%m/%d/%Y:%H:%M:%S")
        outtime = tmp_dt.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        outtime = intime
    return outtime
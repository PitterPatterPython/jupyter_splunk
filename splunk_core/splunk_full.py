#!/usr/bin/python

import datetime
from IPython.core.magic import (magics_class, line_cell_magic)
import ipywidgets as widgets
import pandas as pd
import re
from time import sleep

from splunklib import client as splclient

from splunk_core._version import __desc__
from integration_core import Integration
import jupyter_integrations_utility as jiu

@magics_class
class Splunk(Integration):
    # STATIC VARIABLES
    name_str = "splunk" # The name of the integration
    instances = {} 
    custom_evars = ["splunk_conn_default", "splunk_autologin"]

    # These are the variables in the opts dict that allowed to be set by the user. 
    # These are specific to this custom integration and are joined with the 
    # base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["splunk_conn_default", "splunk_default_earliest_time", "splunk_default_latest_time", "splunk_parse_times", "splunk_autologin"]

    myopts = {}
    myopts["splunk_max_rows"] = [1000, "Max number of rows to return, will potentially add this to queries"]
    myopts["splunk_conn_default"] = ["default", "Default instance to connect with"]
    myopts["splunk_default_earliest_time"] = ["-15m", "The default earliest time sent to the Splunk server"]
    myopts["splunk_default_latest_time"] = ["now", "The default latest time sent to the Splunk server"]
    myopts["splunk_parse_times"] = [1, "If this is 1, it will parse your query for earliest or latest and get the value. It will not alter the query, but update the default earliest/latest for subqueries"]
    myopts["splunk_autologin"] = [True, "Works with the the autologin setting on connect"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Splunk, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()

    def customAuth(self, instance):
        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            jiu.displayMD(f"**[ * ]** Instance **{instance}** not found in instances: Connection Failed")
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst["session"] = None
            mypass = ""
            if inst["enc_pass"] is not None:
                mypass = self.ret_dec_pass(inst["enc_pass"])
                inst["connect_pass"] = ""
            try:
                inst["session"] = splclient.connect(host=inst["host"], port=inst["port"], username=inst["user"], password=mypass, autologin=self.opts["splunk_autologin"][0])
                result = 0
            except:
                jiu.displayMD(f"**[ * ]** Unable to connect to Splunk instance {instance} at {inst['conn_url']}")
                result = -2  

        return result
   
    def validateQuery(self, query, instance):
        """ Warn the user when their query might run into known syntactical issues.

        Keyword arguments:
        query -- the user-supplied query to the cell magic %%splunk
        instance -- the Splunk instance to perform the query against

        Returns:
        allow_run -- boolean that determines if the query should be allowed to run
        """
        
        allow_run = True
        allow_rerun = False

        if self.instances[instance]["last_query"] == query:
            # If the validation allows rerun, that we are here:
            allow_rerun = True
        
        # Validation checks 

        # The query doesn't start with the "search" command (we're using a negative lookahead, future self)
        if re.search(r"^(?!search)", query):
            jiu.displayMD("**[ ! ]** This query doesn't start with the `search` command. \
                          If it fails, try prepending it to the beginning of the query.")
            
        # The query contains the "search" command but doesn't include a "| table *" command
        if re.search(r"^(?=search)", query) and re.search(r"\|\s{0,}table", query) == None:
            jiu.displayMD("**[ ! ]** Your query includes the `search` command but doesn't include the `| table *` command. **This is going to cause issues and is highly recommended that you add this to your query!**")

        # The query contains non-capitalized "AND", "OR", and/or "NOT" operators
        # This case also addresses weird typos like "aND" and "Not" and all their variations
        if re.search(r"\s(and|or|not)\s", query, re.IGNORECASE) and re.search(r"\s(AND|OR|NOT)\s", query) == None:
            jiu.displayMD("**[ ! ]** Your query contains `and` / `or` operators. Splunk requires these to be capitalized. \
                          Review the [query documentation](https://docs.splunk.com/Documentation/SplunkCloud/9.0.2305/Search/Booleanexpressions).")

        # The query contains an open and close bracket
        if re.search(r"[\[\]]", query):
            jiu.displayMD("**[ ! ]** Your query contains square brackets `[ ]`. This might be executed as a \
                           subquery. Double-check your results!")
            if self.opts["splunk_parse_times"][0] == 1:
                jiu.displayMD("**[ ! ]** You are having me parse the queries and set defaults, so if all works \
                              , your earliest and latest are passed to the subquery. (If you passed them!)")
            else:
                jiu.displayMD("**[ ! ]** It doesn't appear you are having me parse query times. \
                               Thus, the earliest and latest ONLY apply to outer most part of your query. Results will be inconsistent")

        # The query doesn't contain the "earliest" parameter
        if re.search(r"earliest", query) == None:
            jiu.displayMD("**[ ! ]** Your query didn't contain the `earliest` parameter. Defaulting to **%s**" % (self.opts[self.name_str + "_default_earliest_time"][0]))

        # The query doesn't contain the "latest" parameter
        if  re.search(r"latest", query) == None:
            jiu.displayMD("**[ ! ]** Your query didn't contain the `latest` parameter. Defaulting to **%s**" % (self.opts[self.name_str + "_default_latest_time"][0]))

        return allow_run

    def parseTimes(self, query):
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

    def splunkTime(self, intime):
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
    
    def customQuery(self, query, instance, reconnect=True):
        """Execute a user supplied Splunk query after a %%splunk cell magic
        
        Keyword arguments:
        query -- the user supplied query
        instance -- the instance to run the user's query against

        Returns:
        dataframe -- the pandas dataframe with the query results
        status -- the final status from the Splunk query
        """

        # Placeholder values while we attempt to determine if the user supplied "earliest" and "latest" params
        earliest_value = None
        latest_value = None

        if self.opts["splunk_parse_times"][0] == 1:
            if self.debug:
                jiu.displayMD("**[ Dbg ]** Attempting to parse `earliest` and `latest` times...")
            
            earliest_value, latest_value = self.parseTimes(query)
            
            if self.debug:
                if earliest_value != None:
                    jiu.displayMD(f"**[ Dbg ]** Found `earliest` value: {earliest_value}")
                else:
                    jiu.displayMD("**[ Dbg ]** Did not find an `earliest` value")

                if latest_value != None:
                    jiu.displayMD(f"**[ Dbg ]** Found `latest` value: {latest_value}")
                else:
                    jiu.displayMD(f"**[ Dbg ]** Did not find a `latest` value")

        if earliest_value is None:
            earliest_value = self.checkvar(instance, "splunk_default_earliest_time")

        if latest_value is None:
            latest_value = self.checkvar(instance, "splunk_default_latest_time")

        earliest_value = self.splunkTime(earliest_value)
        latest_value = self.splunkTime(latest_value)

        kwargs_export = { "earliest_time": earliest_value, 
                            "latest_time": latest_value, 
                            "exec_mode": "normal"
                        }
        if self.debug:
            jiu.displayMD(f"**[ Dbg ]** **kwargs**: {kwargs_export}")
            jiu.displayMD(f"**[ Dbg ]** **query:** {query}")

        dataframe = None
        status = ""
        str_err = ""
        
        # Perform the search
        try:
            search_job = self.instances[instance]["session"].jobs.create(query, **kwargs_export)
            jiu.displayMD(f"**[ * ]** Search job (**{search_job.name}**) has been created")
            jiu.displayMD("**Progress**")
            widgets.Button(description = "Stop")

            while True:
                while not search_job.is_ready():
                    pass

                stats = { "isDone": search_job["isDone"],
                            "doneProgress": float(search_job["doneProgress"])*100,
                            "scanCount": int(search_job["scanCount"]),
                            "eventCount": int(search_job["eventCount"]),
                            "resultCount": int(search_job["resultCount"])
                        }

                print(f"\r\t%(doneProgress)03.1f%%\t\t%(scanCount)d scanned\t\t%(eventCount)d matched\t\t%(resultCount)d results" % stats, end="")

                if stats["isDone"] == "1":
                    jiu.displayMD("**[ * ]** Job has completed!")
                    break

                sleep(1)
            
            if search_job.results is not None:
                dataframe = pd.read_csv(search_job.results(output_mode="csv"))
                str_err = "Success"
            else:
                dataframe = None
                str_err = "Success - No Results"
        
        except Exception as e:
            dataframe = None
            str_err = str(e)

        if str_err.find("Success") >= 0:
            pass
        
        elif str_err.find("No columns to parse from file") >= 0:
            status = "Success - No Results"
            dataframe = None
        
        elif str_err.find("Session is not logged in") >= 0:
        
            # Try to rerun query
            if reconnect == True:
                self.disconnect(instance)
                self.connect(instance)
                m, s = self.customQuery(query, instance, False)
                dataframe = m
                status = s
        
            else:
                dataframe = None
                status = "Failure - Session not logged in and reconnect failed"
        else:
            status = "Failure - query_error: " + str_err
    
        return dataframe, status


    # Display Help can be customized
    def customOldHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp("search term='MYTERM'")

    def retCustomDesc(self):
        return __desc__
        #return "Jupyter integration for working with the Splunk datasource"

    def customHelp(self, curout):
        n = self.name_str
        mn = self.magic_name
        m = "%" + mn
        mq = "%" + m
        table_header = "| Magic | Description |\n"
        table_header += "| -------- | ----- |\n"
        out = curout
        qexamples = []
        qexamples.append(["myinstance", "search term='MYTERM'", "Run a SPL (Splunk) query against myinstance"])
        qexamples.append(["", "search term='MYTERM'", "Run a SPL (Splunk) query against the default instance"])
        out += self.retQueryHelp(qexamples)

        return out

    # This is the magic name.
    @line_cell_magic
    def splunk(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                jiu.displayMD(f"**[ Dbg ]** **line**: {line}")
                jiu.displayMD(f"**[ Dbg ]** **cell**: {cell}")
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    jiu.displayMD(f"You've found the custom testint winning line magic!")
                else:
                    jiu.displayMD(f"I'm sorry, I don't know what you want to do with your line magic, try just {self.name_str} for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)


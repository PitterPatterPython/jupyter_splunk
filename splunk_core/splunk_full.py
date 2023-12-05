#!/usr/bin/python

import datetime
import re
from time import sleep

from IPython.core.magic import (magics_class, line_cell_magic)
from IPython.display import display
import pandas as pd

from splunk_core._version import __desc__
from integration_core import Integration
import jupyter_integrations_utility as jiu

from splunk_utils.splunk_api import SplunkAPI
from splunk_utils.helper_functions import splunk_time, parse_times
from splunk_utils.user_input_parser import UserInputParser
import splunklib.results as results

@magics_class
class Splunk(Integration):
    # STATIC VARIABLES
    name_str = "splunk" # The name of the integration
    instances = {} 
    custom_evars = ["splunk_conn_default", "splunk_autologin"]

    # These are the variables in the opts dict that allowed to be set by the user. 
    # These are specific to this custom integration and are joined with the 
    # base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["splunk_conn_default", "splunk_default_earliest_time", "splunk_default_latest_time", "splunk_parse_times", "splunk_autologin", "splunk_results_count_size"]

    myopts = {}
    myopts["splunk_conn_default"] = ["default", "Default instance to connect with"]
    myopts["splunk_default_earliest_time"] = ["-15m", "The default earliest time sent to the Splunk server"]
    myopts["splunk_default_latest_time"] = ["now", "The default latest time sent to the Splunk server"]
    myopts["splunk_parse_times"] = [1, "If this is 1, it will parse your query for earliest or latest and get the value. It will not alter the query, but update the default earliest/latest for subqueries"]
    myopts["splunk_autologin"] = [True, "Works with the the autologin setting on connect"]
    myopts["splunk_results_count_size"] = [0, "Changing this value from its default - which is not recommended - will limit the number of results that the results reader displays. It does **NOT** limit the number of results in your query (you must set that limit in your Splunk query)"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Splunk, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.user_input_parser = UserInputParser()
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
                inst["session"] = SplunkAPI(host=inst["host"], port=inst["port"], username=inst["user"], password=mypass, autologin=self.opts["splunk_autologin"][0])
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
            
            earliest_value, latest_value = parse_times(query)
            
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

        earliest_value = splunk_time(earliest_value)
        latest_value = splunk_time(latest_value)

        # The "exec_mode" parameter used to be a "myopts" parameter, but we've removed that
        # and hard-coded it to be "normal" since "normal" is the only search type that allows
        # polling for progress. https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/howtousesplunkpython/howtorunsearchespython
        kwargs = { 
                            "earliest_time": earliest_value, 
                            "latest_time": latest_value, 
                            "exec_mode": "normal"
                        }
        if self.debug:
            jiu.displayMD(f"**[ Dbg ]** **kwargs**: {kwargs}")
            jiu.displayMD(f"**[ Dbg ]** **query:** {query}")

        dataframe = None
        status = ""
        str_err = ""
        
        # Perform the search
        try:
            search_job = self.instances[instance]["session"].session.jobs.create(query, **kwargs)
            jiu.displayMD(f"**[ * ]** Search job (**{search_job.name}**) has been created")
            jiu.displayMD("**Progress**")

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
                # dataframe = pd.read_csv(search_job.results(output_mode="csv", count=self.opts["splunk_results_count_size"][0]))
                resultCount = search_job["resultCount"]
                offset = 0
                count = 100
                accumulated_results = []
                
                while (offset < int(resultCount)):
                    kwargs_paginate = {"count": count, "offset": offset, "output_mode": "json"}
                    
                    search_results = search_job.results(**kwargs_paginate)
                    for result in results.JSONResultsReader(search_results):
                        if isinstance(result, dict):
                            accumulated_results.append(result)
                    
                    offset += count
                        
                str_err = "Success"
                dataframe = pd.json_normalize(accumulated_results)
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

    def retQueryHelp(self, q_examples=None):
        # Our current customHelp function doesn't support a table for line magics
        # (it's built in to integration_base.py) so I'm overriding it.
        
        magic_name = self.magic_name
        magic = f"%{magic_name}"
        
        cell_magic_helper_text = (f"\n## Running {magic_name} queries with cell magics\n"
                       "--------------------------------\n"
                       f"\n#### When running {magic} queries with cell magics, {magic} and the instance name will be on the first line of your cell, and then your native {magic} query on the 2nd line.\n"
                       "\n### Cell magic examples\n"
                       "-----------------------\n")
        
        cell_magic_table = ("| Cell Magic | Description |\n"
                            "| ---------- | ----------- |\n"
                            "| \%\%splunk 'instance'<br>'splunk query' | Run a SPL (Splunk) query against myinstance |\n"
                            )
        
        line_magic_helper_text = (f"\n## Running {magic_name} line magics\n"
                                  "-------------------------------\n"
                                  f"\n#### To see a line magic's command syntax, type `%splunk 'name of line magic' -h`\n"
                                  "\n### Line magic examples\n"
                                  "-----------------------\n")
        
        line_magic_table = ("| Line Magic | Description |\n"
                            "| ---------- | ----------- |\n"
                            "| \%splunk update_lookup_table 'options' | Update a lookup table with a dataframe. Type `%splunk update_lookup_table -h` for command syntax. |\n")
        
        help_out = cell_magic_helper_text + cell_magic_table + line_magic_helper_text + line_magic_table
        
        return help_out

    def retCustomDesc(self):
        return __desc__

    def customHelp(self, current_output):
        out = current_output
        out += self.retQueryHelp(None)

        return out

    # This is the magic name.
    @line_cell_magic
    def splunk(self, line, cell=None):
        """Execute a custom line magic against a Splunk instance.
            
            START HERE -- Here's the general flow:
            1.  We need to parse the user's line magic via ../utils/user_input_parser. We
                construct an object there that has metadata. We use that object to drive
                the rest of this function.
            2.  We'll display errors if there were any obvious ones during parsing (this lives)
                on the "errors" key in the object from step 1 above.
            3.  Using the parsed input's "input" object, we'll send those to the Splunk
                API's _handler function via ../utils/splunk_api. The _handler function
                plays traffic cop for every API call. 

        Args:
            line (string): the user's line magic
            cell (None, optional): user's cell magic (this shouldn't ever exist right here).
        """
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            
            if self.debug:
                jiu.displayMD(f"**[ Dbg ]** **line**: {line}")
                jiu.displayMD(f"**[ Dbg ]** **cell**: {cell}")
            
            if not line_handled: # We based on this we can do custom things for integrations. 
                try:
                    parsed_input = self.user_input_parser.parse_input(line)
                    
                    if self.debug:
                        jiu.displayMD(f"**[ Dbg ]** Parsed Query: `{parsed_input}`")
                        
                    if parsed_input["error"] == True:
                        jiu.displayMD(f"**[ ! ]** {parsed_input['message']}")
                        
                    else:
                        instance = parsed_input["input"]["instance"]
                        dataframe = parsed_input["input"]["dataframe"]
                        
                        if instance not in self.instances.keys():
                            jiu.displayMD(f"**[ * ]** Instance **{instance}** not found in instances")
                            
                        elif dataframe not in self.ipy.user_ns.keys():
                            jiu.displayMD(f"**[ * ]** You supplied a dataframe **{dataframe}** that doesn't seem to exist.")
                    
                        else:
                            user_dataframe = self.ipy.user_ns[dataframe]
                            response = self.instances[instance]["session"]._handler(**parsed_input["input"], df=user_dataframe)
                            jiu.displayMD(f"**[ * ]** {response}")
                
                except Exception as e:
                    jiu.displayMD(f"**[ ! ]** There was an error in your line magic: `{e}`")
        
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)


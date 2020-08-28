#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict

from integration_core import Integration

from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML

# Your Specific integration imports go here, make sure they are in requirements!
from splunklib import client as splclient

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Splunk(Integration):
    # Static Variables
    # The name of the integration
    name_str = "splunk"
    custom_evars = [name_str + "_base_url", name_str + "_user"]
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = [name_str + '_base_url', name_str + "_search_mode", name_str + "_default_earliest_time", name_str + "_default_latest_time"] 

    
    myopts = {} 
    myopts[name_str + '_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts[name_str + '_user'] = ["splunk", "User to connect with  - Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_USER otherwise will prompt"]
    myopts[name_str + '_base_url'] = ["", "URL to connect to server. format: splunk://%host:%port - splunk://localhost:8089 for example.  Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_BASE_URL"]
    myopts[name_str + '_base_url_host'] = ["", "Hostname of connection derived from base_url"]
    myopts[name_str + '_base_url_port'] = ["", "Port of connection derived from base_url"]
    myopts[name_str + '_base_url_scheme'] = ["", "Scheme of connection derived from base_url"]


    myopts[name_str + "_default_earliest_time"] = ["-15m", "The default earliest time sent to the Splunk server"]
    myopts[name_str + "_default_latest_time"] = ["now", "The default latest time sent to the Splunk server"]
    myopts[name_str + "_search_mode"] = ["normal", "The search mode sent to the splunk server"]
    myopts[name_str + '_output_mode'] = ["csv", "The output mode sent to the splunk server, don't change this, we rely on it being csv"]
    myopts[name_str + '_last_query'] = ['', "The last query run in splunk"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", splunk_base_url="", debug=False, *args, **kwargs):
        super(Splunk, self).__init__(shell, debug=debug, pd_display_grid=pd_display_grid)
        self.debug = debug

        self.opts['pd_display_grid'][0] = pd_display_grid
        if pd_display_grid == "qgrid":
            try:
                import qgrid
            except:
                print ("WARNING - QGRID SUPPORT FAILED - defaulting to html")
                self.opts['pd_display_grid'][0] = "html"

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]
        self.load_env(self.custom_evars)

        if splunk_base_url != "":
            if self.opts[self.name_str + "_base_url"][0] != "":
                print("Warning: overwriting ENV provided JUPYTER_SPLUNK_BASE_URL with object instantiated splunk_base_url")
            self.opts[self.name_str + "_base_url"][0] = splunk_base_url


    def disconnect(self):
        if self.connected == True:
            print("Disconnected %s Session from %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_base_url'][0]))
        else:
            print("%s Not Currently Connected - Resetting All Variables" % self.name_str.capitalize())
        self.mysession = None
        self.connect_pass = None
        self.connected = False


    def connect(self, prompt=False):
        if self.connected == False:
            if prompt == True or self.opts[self.name_str + '_user'][0] == '':
                print("User not specified in %s%s_USER or user override requested" % (self.env_pre, self.name_str.upper()))
                tuser = input("Please type user name if desired: ")
                self.opts[self.name_str + '_user'][0] = tuser
            print("Connecting as user %s" % self.opts[self.name_str + '_user'][0])
            print("")

            if prompt == True or self.opts[self.name_str  + "_base_url"][0] == '':
                print("Base URL not specified in %s%s_BASE_URL or override requested" % (self.env_pre, self.name_str.upper()))
                turl = input("Please type in the full %s URL: " % self.name_str.capitalize())
                self.opts[self.name_str + '_base_url'][0] = turl
            print("Connecting to %s URL: %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_base_url'][0]))
            print("")

            myurl = self.opts[self.name_str + '_base_url'][0]
            ts1 = myurl.split("://")
            self.opts[self.name_str + '_base_url_scheme'][0] = ts1[0]
            t1 = ts1[1]
            ts2 = t1.split(":")
            self.opts[self.name_str + '_base_url_host'][0] = ts2[0]
            self.opts[self.name_str + '_base_url_port'][0] = ts2[1]

#            Use the following if your data source requries a password
            if self.connect_pass == "":
                print("Please enter the password you wish to connect with:")
                tpass = ""
                self.ipy.ex("from getpass import getpass\ntpass = getpass(prompt='Connection Password: ')")
                tpass = self.ipy.user_ns['tpass']

                self.connect_pass = tpass
                self.ipy.user_ns['tpass'] = ""

                result = self.auth()

            if result == 0:
                self.connected = True
                print("%s - %s Connected!" % (self.name_str.capitalize(), self.opts[self.name_str + '_base_url'][0]))
            else:
                print("Connection Error - Perhaps Bad Usename/Password?")

        elif self.connected == True:
            print(self.name_str.capitalize() + "is already connected - Please type %" + self.name_str + " for help on what you can you do")

        if self.connected != True:
            self.disconnect()

    def auth(self):
        self.session = None
        result = -1

        try:      
            self.session = splclient.connect(host=self.opts[self.name_str + '_base_url_host'][0], port=self.opts[self.name_str + '_base_url_port'][0], username=self.opts[self.name_str + '_user'][0], password=self.connect_pass)
            result = 0
        except:
            print("Unable to connect to Splunk")
            result = -1  

        return result


    def validateQuery(self, query):
        bRun = True
        bReRun = False
        if self.opts[self.name_str + "_last_query"][0] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query 
        self.opts[self.name_str + "_last_query"][0] = query

        # Example Validation

        # Warn only - Don't change bRun
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter
        if query.find("search") != 0:
            print("This query doesn't start with search, if it fails, you may want to add that (it doesn't infer it like the Splunk UI")
            print("")

        if query.find("or") >= 0 or query.find("and") >= 0 or query.find("Or") >= 0 or query.find("And") >= 0: 
            print("Your query contains or, and, Or, or And - Splunk doesn't treat these as operators, and your results may not be what you want")
            print("")

        if query.find("earliest") < 0 or query.find("latest") < 0:
            print("Your query didn't contain the strings earliest or latest, and is likely using the default settings of earliest: %s and latest: %s" % (self.opts[self.name_str + "_default_earliest_time"][0], self.opts[self.name_str + "_default_latest_time"][0]))
            print("")
        # Warn and do not allow submission
        # There is no way for a user to submit this query 
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun

    def customQuery(self, query, reconnect=True):

        self.opts[self.name_str + "_last_query"][0] = query
        kwargs_export = { "earliest_time": self.opts[self.name_str + "_default_earliest_time"][0], "latest_time": self.opts[self.name_str + "_default_latest_time"][0], "search_mode": self.opts[self.name_str + "_search_mode"][0], "output_mode": self.opts[self.name_str + "_output_mode"][0]}
        mydf = None
        status = ""
        try:
            results = self.session.jobs.export(query, **kwargs_export)
            if results is not None:
                mydf = pd.read_csv(results)
        except Exception as e:
            mydf = None
            str_err = str(e)

        if str_err.find("No columns to parse from file") >= 0:
            status = "Success - No Results"
        elif str_err.find("Session is not logged in") >= 0:
            # Try to rerun query
            if reconnect == True:
                self.disconnect()
                self.connect()
                m, s = self.customQuery(query, False)
                mydf = m
                status = s
            else:
                mydf = None
                status = "Failure - Session not logged in and reconnect failed"
        else:
            status = "Failure - query_error: " + str_err
    
        return mydf, status


# Display Help must be completely customized, please look at this Hive example
    def customHelp(self):
        print("jupyter_spluk is a interface that allows you to use the magic function %splunk to interact with an Splunk installation.")
        print("")
        print("jupyter_splunk has two main modes %splunk and %%splunk")
        print("%splunk is for interacting with a Splunk installation, connecting, disconnecting, seeing status, etc")
        print("%%splunk is for running queries and obtaining results back from the Splunk cluster")
        print("")
        print("%splunk functions available")
        print("###############################################################################################")
        print("")
        print("{: <30} {: <80}".format(*["%splunk", "This help screen"]))
        print("{: <30} {: <80}".format(*["%splunk status", "Print the status of the Splunk connection and variables used for output"]))
        print("{: <30} {: <80}".format(*["%splunk connect", "Initiate a connection to the Splunk cluster, attempting to use the ENV variables for Splunk URL and Splunk Username"]))
        print("{: <30} {: <80}".format(*["%splunk connect alt", "Initiate a connection to the Splunk cluster, but prompt for Username and URL regardless of ENV variables"]))
        print("{: <30} {: <80}".format(*["%splunk disconnect", "Disconnect an active Splunk connection and reset connection variables"]))
        print("{: <30} {: <80}".format(*["%splunk set %variable% %value%", "Set the variable %variable% to the value %value%"]))
        print("{: <30} {: <80}".format(*["%splunk debug", "Sets an internal debug variable to True (False by default) to see more verbose info about connections"]))
        print("")
        print("Running queries with %%splunk")
        print("###############################################################################################")
        print("")
        print("When running queries with %%splunk, %%splunk will be on the first line of your cell, and the next line is the query you wish to run. Example:")
        print("")
        print("%%splunk")
        print('search term="MYTERM"')
        print("")
        print("Some query notes:")
        print("- If the number of results is less than pd_display.max_rows, then the results will be diplayed in your notebook")
        print("- You can change pd_display.max_rows with %splunk set pd_display.max_rows 2000")
        print("- The results, regardless of display will be place in a Pandas Dataframe variable called prev_splunk")
        print("- prev_splunk is overwritten every time a successful query is run. If you want to save results assign it to a new variable")




    # This is the magic name.
    @line_cell_magic
    def splunk(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + "for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell)


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
    instances = {} 
    custom_evars = ['splunk_conn_default']
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base

    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["splunk_conn_default", "splunk_search_mode", "splunk_default_earliest_time", "splunk_default_latest_time"] 

    
    myopts = {} 
    myopts['splunk_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts['splunk_conn_default'] = ["default", "Default instance to connect with"]


    myopts["splunk_default_earliest_time"] = ["-15m", "The default earliest time sent to the Splunk server"]
    myopts["splunk_default_latest_time"] = ["now", "The default latest time sent to the Splunk server"]
    myopts["splunk_search_mode"] = ["normal", "The search mode sent to the splunk server"]
    myopts['splunk_output_mode'] = ["csv", "The output mode sent to the splunk server, don't change this, we rely on it being csv"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", splunk_conn_url_default="", debug=False, *args, **kwargs):
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
        if splunk_conn_url_default != "":
            if "default" in self.instances.keys():
                print("Warning: default instance in ENV and passed to class creation - overwriting ENV")
            self.fill_instance("default", splunk_conn_url_default)

        self.parse_instances()



    def customAuth(self, instance):
        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            print("Instance %s not found in instances - Connection Failed" % instance)
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst['session'] = None
            mypass = ""
            if inst['connect_pass'] is not None:
                mypass = inst['connect_pass']
            else:
                mypass = self.instances[self.opts[self.name_str + "_conn_default"][0]]['connect_pass']
            try:
                inst['session'] = splclient.connect(host=inst['host'], port=inst['port'], username=inst['user'], password=mypass)
                result = 0
            except:
                print("Unable to connect to Splunk instance %s at %s" % (instance, inst["conn_url"]))
                result = -2  

        return result


    def validateQuery(self, query, instance):
        bRun = True
        bReRun = False

        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query 
        self.instances[instance]['last_query'] = query
        # Example Validation

        # Warn only - Don't change bRun
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter
        if query.find("search") != 0:
            print("This query doesn't start with search, if it fails, you may want to add that (it doesn't infer it like the Splunk UI)")
            print("")

        if query.find(" or ") >= 0 or query.find(" and ") >= 0 or query.find(" Or ") >= 0 or query.find(" And ") >= 0: 
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

    def customQuery(self, query, instance, reconnect=True):

        kwargs_export = { "earliest_time": self.checkvar(instance, 'splunk_default_earliest_time'), "latest_time": self.checkvar(instance, "splunk_default_latest_time"), "search_mode": self.checkvar(instance, "splunk_search_mode"), "output_mode": self.checkvar(instance, "splunk_output_mode")}
        mydf = None
        status = ""
        str_err = ""
        try:
            results = self.instances[instance]['session'].jobs.export(query, **kwargs_export)
            if results is not None:
                mydf = pd.read_csv(results)
                str_err = "Success"
            else:
                mydf = None
                str_err = "Success - No Results"
        except Exception as e:
            mydf = None
            str_err = str(e)

        if str_err.find("Success") >= 0:
            pass
        elif str_err.find("No columns to parse from file") >= 0:
            status = "Success - No Results"
            mydf = None
        elif str_err.find("Session is not logged in") >= 0:
            # Try to rerun query
            if reconnect == True:
                self.disconnect(instance)
                self.connect(instance)
                m, s = self.customQuery(query, instance, False)
                mydf = m
                status = s
            else:
                mydf = None
                status = "Failure - Session not logged in and reconnect failed"
        else:
            status = "Failure - query_error: " + str_err
    
        return mydf, status


# Display Help can be customized
    def customHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp('search term="MYTERM"')

 




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
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)


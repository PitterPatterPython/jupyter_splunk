#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
import re
from integration_core import Integration
import datetime
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML
from splunk_core._version import __desc__

# Your Specific integration imports go here, make sure they are in requirements!
from splunklib import client as splclient
import jupyter_integrations_utility as jiu
#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Splunk(Integration):
    # Static Variables
    # The name of the integration
    name_str = "splunk"
    instances = {} 
    custom_evars = ['splunk_conn_default', 'splunk_autologin']
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base

    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["splunk_conn_default", "splunk_search_mode", "splunk_default_earliest_time", "splunk_default_latest_time", "splunk_parse_times", "splunk_autologin"]


    myopts = {}
    myopts['splunk_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts['splunk_conn_default'] = ["default", "Default instance to connect with"]

    myopts["splunk_default_earliest_time"] = ["-15m", "The default earliest time sent to the Splunk server"]
    myopts["splunk_default_latest_time"] = ["now", "The default latest time sent to the Splunk server"]
    myopts["splunk_parse_times"] = [1, "If this is 1, it will parse your query for earliest or latest and get the value. It will not alter the query, but update the default earliest/latest for subqueries"]
    myopts["splunk_search_mode"] = ["normal", "The search mode sent to the splunk server"]
    myopts['splunk_output_mode'] = ["csv", "The output mode sent to the splunk server, don't change this, we rely on it being csv"]
    myopts['splunk_autologin'] = [True, "Works with the the autologin setting on connect"]

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
            print("Instance %s not found in instances - Connection Failed" % instance)
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst['session'] = None
            mypass = ""
            if inst['enc_pass'] is not None:
                mypass = self.ret_dec_pass(inst['enc_pass'])
                inst['connect_pass'] = ""
            try:
                inst['session'] = splclient.connect(host=inst['host'], port=inst['port'], username=inst['user'], password=mypass, autologin=self.opts['splunk_autologin'][0])
                result = 0
            except:
                print("Unable to connect to Splunk instance %s at %s" % (instance, inst["conn_url"]))
                result = -2  

        return result

   
    def validateQuery(self, query, instance):
        ''' Warn only - Don't change bRun. Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter'''

        bRun = True
        bReRun = False

        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        
        # Validation checks 

        # The query doesn't start with the "search" command (we're using a negative lookahead, future self)
        if re.search(r'^(?!search)', query):
            jiu.displayMD("**[ ! ]** This query doesn't start with the `search` command. \
                          If it fails, try prepending it to the beginning of the query.")

        # The query contains non-capitalized "AND", "OR", and/or "NOT" operators
        # This case also addresses weird typos like "aND" and "Not" and all their variations
        if re.search(r'\s(and|or|not)\s', query, re.IGNORECASE) and re.search(r'\s(AND|OR|NOT)\s', query) == None:
            jiu.displayMD("**[ ! ]** Your query contains `and` / `or` operators. Splunk requires these to be capitalized. \
                          Review the [query documentation](https://docs.splunk.com/Documentation/SplunkCloud/9.0.2305/Search/Booleanexpressions).")

        # The query contains an open and close bracket
        if re.search(r'[\[\]]', query):
            jiu.displayMD("**[ ! ]** Your query contains square brackets `[ ]`. This might be executed as a \
                           subquery. Double-check your results!")
            if self.opts['splunk_parse_times'][0] == 1:
                jiu.displayMD("**[ ! ]** You are having me parse the queries and set defaults, so if all works \
                              , your earliest and latest are passed to the subquery. (If you passed them!)")
            else:
                jiu.displayMD("**[ ! ]** It doesn't appear you are having me parse query times. \
                               Thus, the earliest and latest ONLY apply to outer most part of your query. Results will be inconsistent")

        # The query doesn't contain the "earliest" keyword
        if re.search(r'earliest', query) == None:
            jiu.displayMD("**[ ! ]** Your query didn't contain the `earliest` parameter. Defaulting to **%s**" % (self.opts[self.name_str + "_default_earliest_time"][0]))

        if  re.search(r'latest', query) == None:
            jiu.displayMD("**[ ! ]** Your query didn't contain the `latest` parameter. Defaulting to **%s**" % (self.opts[self.name_str + "_default_latest_time"][0]))

        return bRun

    def parseTimes(self, query):
        e_ret = None
        l_ret = None
        e_match = re.search(r"earliest ?= ?[\"\']?([^\s\'\"]+)[\s\"\']", query)
        if e_match:
            e_ret = e_match.group(1)
        l_match = re.search(r"latest ?= ?[\"\']?([^\s\'\"]+)[\s\"\']", query)
        if l_match:
            l_ret = l_match.group(1)
        return e_ret, l_ret



    def splunkTime(self, intime):
        # Converts the normal shitty splunk time to the other format it requires in the API
        m = re.search("\d{1,2}\/\d{1,2}\/\d{4}", intime)

        if m:
            tmp_dt = datetime.datetime.strptime(intime, "%m/%d/%Y:%H:%M:%S")
            outtime = tmp_dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            outtime = intime
        return outtime
    def customQuery(self, query, instance, reconnect=True):

        e_val = None
        l_val = None
        if self.opts["splunk_parse_times"][0] == 1:
            if self.debug:
                print("Attempting to parse earliest and latest times")
            e_val, l_val = self.parseTimes(query)
            if self.debug:
                print("Value of Earliest parsed from query: %s" % e_val)
                print("Value of Latest parsed from query: %s" % l_val)


        if e_val is None:
            e_val = self.checkvar(instance, 'splunk_default_earliest_time')
        if l_val is None:
            l_val = self.checkvar(instance, "splunk_default_latest_time")
        e_val = self.splunkTime(e_val)
        l_val = self.splunkTime(l_val)

        kwargs_export = { "earliest_time": e_val, "latest_time": l_val, "search_mode": self.checkvar(instance, "splunk_search_mode"), "output_mode": self.checkvar(instance, "splunk_output_mode")}
        if self.debug:
            print("kwargs: %s" % kwargs_export)
            print("query: %s" % query)

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
    def customOldHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp('search term="MYTERM"')

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
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)


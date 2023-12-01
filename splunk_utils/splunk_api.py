from splunklib import client as splclient
import splunklib.results as results
from time import sleep
import jupyter_integrations_utility as jiu
from splunk_utils.helper_functions import parse_times, splunk_time

class SplunkAPI:
    
    def __init__(self, host, port, username, password, autologin):
        self.session = splclient.connect(
            host=host, 
            port=port, 
            username=username, 
            password=password, 
            autologin=autologin
        )
        
    def _handler(self, command, **kwargs):
        """Brokers Splunk API commands on behalf of the calling function

        Args:
            command (string): the function within this class to run
            **kwargs (dict): additional arguments to pass along to the
                functions within this class

        Returns:
            passes through a response from the functions below
        """
        return getattr(self, command)(**kwargs)
    
    def get_lookup_table_field_names(self, lookup_table_name):
        """Retrieve the field names of a lookup table in Splunk

        Args:
            lookup_table_name (string): the name of the lookup table in Splunk

        Returns:
            cols: a list of field names from the lookup table in Splunk
        """
        kwargs = { "earliest_time": "-1m",
                  "latest_time": "now",
                  "search_mode": "normal",
                  "output_mode": "json"}
        
        query = f"| inputlookup {lookup_table_name} | stats dc(*) as * | transpose | table column"
        
        job = self.session.jobs.export(query, **kwargs)
        cols = [each["column"] for each in results.JSONResultsReader(job) if isinstance(each, dict)]
        return cols
    
    def update_lookup_table(self, **kwargs):
        """Update a lookup table with a dataframe from Jupyter

        Returns:
            (string): a simple string containing a success or error message
        """

        table = kwargs.get("table")
        nocheck = kwargs.get("nocheck")
        user_dataframe = kwargs.get("df")
        
        # If the user wants to check that their dataframe column names match up 
        # with the field names in the Splunk lookup table, we'll do that here.
        # If they don't, immediately return an error message and stop processing.
        if nocheck == False:
            try:
                df_column_names = user_dataframe.columns.values.tolist()
                lookup_table_field_names = self.get_lookup_table_field_names(table)
                
                if any(col not in lookup_table_field_names for col in df_column_names):
                    return "There are column names in your dataframe that don't map to field names \
                        in your lookup table, so we're not going to run this command. You can override \
                        this by passing the --nocheck flag, but this really, _really_ isn't \
                        recommended unless you know what you're doing."
                
            except Exception:
                raise
        
        # Run the lookup table update command    
        try:
            user_dataframe_as_csv = user_dataframe.to_csv(index=False)
            
            kwargs_normal = { 
                "earliest_time": "-1m", 
                "latest_time": "now", 
                "exec_mode": "normal"
            }
            
            lookup_table_append_query = (f"| inputlookup {table}"
                     f"| append [makeresults format=csv data=\"{user_dataframe_as_csv}\"]"
                     "| uniq"
                     f"| outputlookup {table}"
            )
            
            job = self.session.jobs.create(lookup_table_append_query, **kwargs_normal)
            jiu.displayMD(f"**[ * ]** Search job (**{job.name}**) has been created")
            jiu.displayMD("**Progress**")
            
            while True:
                while not job.is_ready():
                    pass

                stats = { 
                         "isDone": job["isDone"],
                         "doneProgress": float(job["doneProgress"])*100
                }

                print(f"\r\t%(doneProgress)03.1f" % stats, end="")

                if stats["isDone"] == "1":
                    break

                sleep(1)
            
            return "**[ * ]** Job has completed!"
        
        except Exception:
            raise
            
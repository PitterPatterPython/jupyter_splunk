from argparse import ArgumentParser, BooleanOptionalAction
from splunk_utils.splunk_api import SplunkAPI

class UserInputParser(ArgumentParser):
    """A class to parse a user's line magic from Jupyter
    """
    
    def __init__(self, *args, **kwargs):
        self.valid_commands = list(filter(lambda func : not func.startswith('_') and hasattr(getattr(SplunkAPI,func),'__call__') , dir(SplunkAPI)))
        self.parser = ArgumentParser(prog=r"%splunk")
        
        self.subparsers = self.parser.add_subparsers(dest="command")
        
        # Subparser for "update_lookup_table" command
        self.parser_update_lookup_table = self.subparsers.add_parser("update_lookup_table", help="Update a lookup table in Splunk")
        self.parser_update_lookup_table.add_argument("-i", "--instance", required=True, help="the instance to run the command against")
        self.parser_update_lookup_table.add_argument("-t", "--table", required=True, help="the lookup table to append to")
        self.parser_update_lookup_table.add_argument("-d", "--dataframe", required=True, help="the dataframe to append to the lookup table")
        self.parser_update_lookup_table.add_argument("--nocheck", default=False, action=BooleanOptionalAction, required=False, help="use this flag if you don't care about checking that your column headers match the field names in the Splunk lookup table (NOT RECOMMENDED!)")
        
    def display_help(self, command):
        self.parser.parse_args([command, "--help"])
        
    def parse_input(self, input):
        """Parses the user's line magic from Jupyter

        Args:
            input (_type_): the entire contents of the line from Jupyter

        Returns:
            parsed_input (dict): an object containing an error status, a message,
                and parsed command from argparse.parse()
        """
        parsed_input = {
            "error" : False,
            "message" : None,
            "input" : {}
        }
        
        try:
            if len(input.strip().split("\n")) > 1:
                parsed_input["error"] = True
                parsed_input["message"] = r"The line magic is more than one line and shouldn't be. Try `%splunk --help` or `%splunk -h` for proper formatting"
            
            else:
                parsed_user_command = self.parser.parse_args(input.split())
                parsed_input["input"].update(vars(parsed_user_command))
        
        except SystemExit:
            parsed_input["error"] = True
            parsed_input["message"] = r"Invalid input received, see the output above. Try `%splunk --help` or `%splunk -h`"
        
        return parsed_input
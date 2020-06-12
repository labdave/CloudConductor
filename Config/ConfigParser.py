import logging

from Config.Parsers import JsonParser
from Config.Parsers import CfgParser

class ConfigParser(object):
    # Class for parsing, validating, and storing configuration information from external file
    # Config file can be either JSON or ConfigObj format
    def __init__(self, config_file, config_spec_file):
        # Parse config and validate config file

        # Supported ConfigParser classes associated with each file extension
        config_parser_types = {
            ".cfg"   : CfgParser,
            ".config": CfgParser,
            ".json"  : JsonParser,
            ".jsn"   : JsonParser
        }

        # Detect config format from extension and parse config with associated ConfigParser
        self.config_parser = None
        for extension, config_class in config_parser_types.items():
            if config_file.lower().endswith(extension):
                self.config_parser = config_class(config_file, config_spec_file)
                break

        # Throw error if config file format could not be inferred from file extension
        if self.config_parser is None:
            logging.error("Could not detect the config file format! Accepted file extensions: %s" % ", ".join(list(config_parser_types.keys())))
            raise IOError("Could not detect the config file format!")

        self.config = self.config_parser.get_config()

    def get_config(self):
        return self.config

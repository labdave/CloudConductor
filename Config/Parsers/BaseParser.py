import os
import sys
import logging
import abc

class BaseParser(object, metaclass=abc.ABCMeta):
    def __init__(self, config_file, config_spec_file):

        # Obtain the path to config file (part of the current directory)
        if os.path.isabs(config_file):
            self.config_file = config_file
        else:
            self.config_file = os.path.join(os.getcwd(), config_file)

        # Obtain the path to config validation file (part of the source code main directory)
        if os.path.isabs(config_spec_file):
            self.config_spec_file = config_spec_file
        else:
            self.config_spec_file = os.path.join(
                os.path.dirname(
                    os.path.dirname(
                        os.path.dirname(__file__)
                    )
                ),
                config_spec_file
            )

        # Check to make sure config file and config spec file actually exist
        self.__check_files()

        # Parse and validate config file and store as dictionary object
        self.config = self.__get_valid_config()

    def __check_files(self):
        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            logging.error("Config file not found: %s" % self.config_file)
            raise IOError("Config file not found!")

        # Checking if the config schema file exists
        if not os.path.isfile(self.config_spec_file):
            logging.error("Config specification file not found: %s" % self.config_spec_file)
            raise IOError("Config spec. file not found!")

    @abc.abstractmethod
    def read_config(self):
        pass

    @abc.abstractmethod
    def validate_config(self):
        pass

    def __get_valid_config(self):
        self.config = self.read_config()
        self.validate_config()
        return self.config

    def get_config(self):
        return self.config





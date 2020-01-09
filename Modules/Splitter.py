import logging
import os
from collections import OrderedDict

from System.Datastore import GAPFile
from Modules import Module

class Splitter(Module):

    def __init__(self, module_id, is_docker=False):

        # Initialize object
        super(Splitter, self).__init__(module_id, is_docker)
        self.output = OrderedDict()

    def define_input(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_input()' function!" % self.__class__.__name__)

    def define_output(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_output()' function!" % self.__class__.__name__)

    def define_command(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_command()' function!" % self.__class__.__name__)

    def make_split(self, split_id, visible_samples=None):
        # Create new split with id and the samples visible (None=all samples)
        if split_id in self.output:
            logging.error("Module of type '%s' declared split with duplicate id (%s)!" % (self.__class__.__name__, split_id))
            raise RuntimeError("Module declared split with duplicate id!")

        # Convert visible samples to list if its a string so we can always assume its a list
        if isinstance(visible_samples, str):
            visible_samples = [visible_samples]

        # Create split with the following samples visible
        self.output[split_id] = {"visible_samples" : visible_samples}

    def add_output(self, split_id, key, value, is_path=True, **kwargs):

        # Check that split id exists
        if split_id not in self.output:
            logging.error("In module %s, cannot add output of type '%s' to undeclared split with id '%s'!" % (self.module_id, key, split_id))
            raise RuntimeError("Splitter attempted to add output to undeclared split!")

        # Check that output type hasn't already been set
        if key in self.output[split_id]:
            logging.error("In module %s, the output key '%s' is defined multiple times in same split (split_id: %s)!" % (self.module_id, key, split_id))
            raise RuntimeError("Output key '%s' has been defined multiple times within the same split!" % key)

        # Convert paths to GAPFiles if they haven't already been converted
        if is_path and not isinstance(value, GAPFile) and value is not None:
            self.output[split_id][key] = self.convert_to_gapfile(split_id, key, value, **kwargs)
        # Otherwise just set the value
        else:
            self.output[split_id][key] = value

    def get_output(self, split_id=None, key=None):
        if split_id is None:
            return self.output
        elif key is None:
            return self.output[split_id]
        return self.output[split_id][key]

    def set_output(self, split_id, key, value):
        # Set value for output object
        if key not in self.output_keys:
            logging.error("Attempt to set undeclared output type '%s' for module with id '%s' of type %s" % (key,
                                                                                                             self.module_id,
                                                                                                             self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared output type for module!")

        if split_id not in self.output:
            logging.error("Attempt to set undeclared output type '%s' for module with id '%s' of type %s" % (key,
                                                                                                             self.module_id,
                                                                                                             self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared output type for module!")
        self.output[split_id][key] = value

    def generate_unique_file_name(self, split_id, extension=".dat", output_dir=None):

        # Generate file basename
        path = "%s.%s" % (self.module_id, split_id)

        # Standardize and append extension
        extension = ".%s" % extension.lstrip(".")
        path += extension

        # Join with output dir other than self.output_dir
        if output_dir is not None:
            path = os.path.join(output_dir, path)
        # Otherwise default to creating output files in self.output_dir
        else:
            path = os.path.join(self.output_dir, path)

        return path

    def generate_gapfile(self, split_id, key, value, **kwargs):
        # Generate file id
        file_id = "%s.%s.%s" % (self.module_id, split_id, key)

        return GAPFile(file_id, file_type=key, path=value, **kwargs)

    def convert_to_gapfile(self, _split_id, _key, _value, **_kwargs):
        # Recursively convert all paths to GAPFile
        if not isinstance(_value, list):
            return self.generate_gapfile(_split_id, _key, _value, **_kwargs)
        else:
            return [self.convert_to_gapfile(_split_id, _key, _file, **_kwargs) for _file in _value]

    def get_output_values(self):
        # Get list of current output values from all splits aggregated into single list
        outputs = []
        for split_id, split_ouptut in self.output.items():
            outputs.extend(list(split_ouptut.values()))
        return outputs


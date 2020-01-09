import logging

from Config import ConfigParser
from System.Datastore import GAPFile

class Sample(object):

    def __init__(self, sample_data):
        self.id    = sample_data.pop("sample_id")
        self.name  = sample_data.pop("sample_name")
        self.paths = sample_data.pop("paths")
        self.data  = sample_data
        self.__make_gap_files()

    def __make_gap_files(self):
        # Convert path strings to GAPFile objects
        for path_type, paths in self.paths.items():
            # More than one path of same type
            if isinstance(paths, list):
                for i in range(len(paths)):
                    file_id = "%s_%s_%s" % (self.get_id(), path_type, i)
                    self.paths[path_type][i] = GAPFile(file_id, path_type, paths[i], sample_name=self.get_id())
            # One path of a given type
            else:
                file_id = "%s_%s_1" % (self.get_id(), path_type)
                self.paths[path_type] = GAPFile(file_id, path_type, paths, sample_name=self.get_id())

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def get_paths(self):
        return self.paths

    def get_data(self):
        return self.data

class SampleSet(object):
    # Container class that parses, holds, and provides access to Sample-level data declared in an external config file
    def __init__(self, sample_data_json):

        # Parse and validate SampleSet config file
        sample_data_spec     = "System/Datastore/SampleSet.validate"
        config_parser        = ConfigParser(sample_data_json, sample_data_spec)
        self.config          = config_parser.get_config()

        # Create Sample Objects
        self.samples   = self.__create_samples()

        # Check that sample-level metadata types are identical for every sample
        self.__check_samples()

        # Get types of data available
        self.__file_types           = list(self.samples[0].get_paths().keys())
        self.__sample_data_types    = list(self.samples[0].get_data().keys())
        self.__global_data_types    = [x for x in list(self.config.keys()) if x != "sample"]

        # Sample order
        self.sample_names = [sample.id for sample in self.samples]

        # Organize global and sample-level metadata by data type
        self.data   = self.__organize_data_by_type()

    def get_num_samples(self):
        return len(self.sample_names)

    def has_data_type(self, data_type):
        # Return true if data type exists in sample data
        return data_type in self.data

    def get_paths(self, path_type=None, samples=None):
        # Return sample data files
        # Optionally can subset by file type and samples. Default is to return all paths from all samples.
        path_type = self.__file_types if path_type is None else path_type
        samples = self.sample_names if samples is None else samples

        # Subset by type
        paths = self.__subset_by_type(self.data, path_type)

        # Subset by sample
        paths = self.__subset_by_sample(paths, samples)

        return paths

    def get_data(self, data_type=None, samples=None):
        # Return sample data
        # Optionally can subset by data type and samples. Default is all data types from all samples.

        if data_type is None and samples is None:
            return self.data

        # Subset by sample
        data = self.data if samples is None else self.__subset_by_sample(self.data, samples)

        if data_type is None:
            return data

        return data[data_type]

    def __create_samples(self):
        # Parse sample data list in config and convert to Sample objects
        # Return dictionary of samples indexed by sample name
        samples = []
        for sample_data in self.config["samples"]:
            samples.append(Sample(sample_data))
        return samples

    def __check_samples(self):
        # Check that all samples contain the same sample-level metadata types
        first = True
        required_data = None
        for sample in self.samples:
            metadata_keys = "".join(sorted(sample.get_data().keys()))
            if first:
                required_data = metadata_keys
                first = False
            elif metadata_keys != required_data:
                # Check that all samples contain the same metadata
                    logging.error("Samples provided in SampleInputConfig have different sample-level metadata types! "
                                  "\nSamples must contain identical metadata types!")
                    raise IOError("One or more samples contains metadata not shared by all other samples!")

    def __subset_by_type(self, data, data_types):
        # Subset data to include only certain data types
        if isinstance(data_types, list):
            # Multiple data types
            return {key : data[key] for key in data_types}
        # Single data type
        return {data_types : data[data_types]}

    def __subset_by_sample(self, data, samples):
        # Subset data to include only certain samples

        # Coerce single sample to list
        if isinstance(samples, str):
            samples = [samples]

        sample_indices = [self.sample_names.index(sample) for sample in samples]
        new_data = {}
        for data_type in data:
            if len(samples) > 1:
                new_data[data_type] = []
                for sample_index in sample_indices:
                    new_data[data_type].append(data[data_type][sample_index])
            elif isinstance(data[data_type], list):
                new_data[data_type] = data[data_type][sample_indices[0]]
            else:
                new_data[data_type] = data[data_type]
        return new_data

    def __organize_data_by_type(self):
        # Combine global and sample-level data into single dictionary organized by data type
        data = dict()

        # Add sample-level data
        for sample in self.samples:
            # Add sample name and ID to data
            self.__add_data(data, "sample_name", sample.get_name())
            self.__add_data(data, "sample_id", sample.get_id())

            # Add sample-level metadata
            for sample_data_type, sample_data_val in sample.get_data().items():
                self.__add_data(data, sample_data_type, sample_data_val)

            # Add sample paths
            for sample_path_type, sample_path in sample.get_paths().items():
                self.__add_data(data, sample_path_type, sample_path)

        # Add any data not associated with a sample as global metadata
        for global_data_type, global_data_val in self.config.items():
            if global_data_type != "samples":
                self.__add_data(data, global_data_type, global_data_val)

        return data

    def __add_data(self, data, key, value):
        # Add value of a data type to a dictionary
        if key in data:
            # If data type already appears in
            if(isinstance(data[key], list)):
                # Add to list that already exists
                data[key].append(value)
            else:
                # Create list from singleton value and add new value to list
                data[key] = [data[key], value]

        elif not isinstance(value, list):
            data[key] = value

        else:
            data[key] = [value]

        return data


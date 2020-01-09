import os
import logging

class GAPFileMetadataError(Exception):
    # Base class for exception related to trying to access unavailable file metadata
    pass

class GAPFile(object):
    # Hold GAP-Related file information
    def __init__(self, file_id, file_type, path, **kwargs):

        # File Id
        self.file_id = file_id

        # Object type e.g. 'samtools', 'ref'
        self.type = file_type

        # Object data
        self.path = path

        # Check to make sure value is string (Path)
        assert isinstance(self.path, str), "GAPFile value must be string! Recieved '%s' of type '%s'" % (path, type(path))

        # Path to a containing directory where resource is found
        self.containing_dir = kwargs.pop("containing_dir", None)

        # Boolean flag for whether path refers to an actual file or is a basename for a set of files
        # E.g. BWA index files may be listed as /path/to/bwa_index and refer to a set of files sharing a common suffix
        self.__is_prefix = self.path.endswith("*")

        # File size
        self.size = kwargs.pop("file_size", None)

        # Sample information
        self.sample_name = kwargs.pop("sample_name", None)

        # Standardize aspects of the resource path provided
        self.__standardize()

        # Metadata associated with an object
        self.metadata = kwargs

        # Flags
        self.flags = []

    @property
    def filename(self):
        return self.path.rstrip("/").split("/")[-1]

    @property
    def containing_dir_name(self):
        if self.containing_dir is not None:
            return self.containing_dir.rstrip("/").split("/")[-1]
        return None

    @property
    def protocol(self):
        if ":" not in self.path:
            return "Local"
        return self.path.split(":")[0]

    def get_file_id(self):
        return self.file_id

    def get_path(self):
        return self.path

    def get_type(self):
        return self.type

    def get_transferrable_path(self):
        # Get path as it should appear if trying to move file to new location

        # Files in containing directories should be moved inside their containing directory
        if self.containing_dir is not None:
            return self.containing_dir

        # Wildcard files should be moved together
        elif self.__is_prefix:
            return self.path + "*"

        # All other files should be moved as a single file
        return self.path

    def get_filename(self):
        return os.path.basename(self.path)

    def get_containing_dir(self):
        return self.containing_dir

    def get_size(self):
        return self.size

    def get_protocol(self):
        return self.protocol

    def is_prefix(self):
        return self.__is_prefix

    def is_remote(self):
        return ":" in self.path if self.containing_dir is None else ":" in self.containing_dir

    def size_known(self):
        # Return true if size is known
        return self.size is not None

    def set_size(self, file_size):
        # Set file size (GB)
        self.size = file_size

    def flag(self, flag_type):
        if flag_type not in self.flags:
            self.flags.append(flag_type)

    def unflag(self, flag_type):
        if self.is_flagged(flag_type):
            self.flags.remove(flag_type)

    def is_flagged(self, flag_type):
        return flag_type in self.flags

    def has_metadata_type(self, meta_type):
        return meta_type in self.metadata

    def get_metadata(self, meta_type):
        if not meta_type in self.metadata:
            logging.error("GAPObject '%s' of type '%s' doesn't have metadata of type '%s'" % (self.file_id, self.type, meta_type))
            raise GAPFileMetadataError("GAPObject does not have metadata of type '%s'" % meta_type)
        return self.metadata[meta_type]

    def set_metadata(self, meta_type, val):
        self.metadata[meta_type] = val

    def set_path(self, new_path):
        self.path = new_path

    def update_path(self, new_dir, new_filename=None):
        # Updates path assuming file has been moved to a new directory
        if new_filename is not None:
            self.path = os.path.join("/".join(self.path.rstrip("/").split("/")[:-1]), new_filename)

        if self.containing_dir is not None:
            # Assume file has been moved inside containing dir
            self.__update_containing_dir(new_dir)
        else:
            self.path = os.path.join(new_dir, self.filename)
        self.__standardize()

    def __update_containing_dir(self, dest_dir):
        # Updates path assuming entire containing directory has been moved to a new directory
        new_path = os.path.join(dest_dir, self.containing_dir_name)
        self.path = self.path.replace(self.containing_dir, "")
        self.containing_dir = new_path

    def __standardize(self):
        if self.containing_dir is not None:
            # Standardize file path of containing directory
            self.containing_dir = self.containing_dir.rstrip("/") + "/"

            # Make all path absolute from containing directory
            file_name = self.path.replace(self.containing_dir, "")
            self.path = os.path.join(self.containing_dir, file_name)

        if self.is_prefix():
            # Remove wildcard character from path is path is prefix
            self.path = self.path.replace("*", "")

    def __str__(self):
        return self.path

    def debug_string(self):
        to_return = "=============\n"
        to_return += "FileID:\t%s\n" % self.file_id
        to_return += "Type:\t%s\n" % self.type
        to_return += "Path:\t%s\n" % self.path
        to_return += "Containing_dir:\t%s\n" % self.containing_dir
        to_return += "Protocol:\t%s\n" % self.protocol
        to_return += "is_remote:\t%s\n" % self.is_remote()
        to_return += "is_prefix:\t%s\n" % self.__is_prefix
        to_return += "size:\t%s\n" % self.size
        to_return += "flags:\t%s\n" % ",".join(self.flags)
        to_return += "=============\n"
        return to_return



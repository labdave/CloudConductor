import logging
import os

from Aries.storage import StorageFile, StoragePrefix, StorageFolder

from System.Platform import CloudPlatform


class InvalidStorageTypeError(Exception):
    pass


class StorageHelper(object):
    # Class designed to facilitate remote file manipulations for a processor

    def __init__(self, proc):
        self.proc = proc

    def mv(self, src_path, dest_path, job_name=None, log=True, wait=False, **kwargs):
        # Transfer file or dir from src_path to dest_path
        # Log the transfer unless otherwise specified
        cmd_generator = StorageHelper.__get_storage_cmd_generator(src_path, dest_path)
        cmd = cmd_generator.mv(src_path, dest_path)

        job_name = f"mv_{CloudPlatform.generate_unique_id()}" if job_name is None else job_name

        # Optionally add logging
        cmd = f"{cmd} !LOG3!" if log else cmd

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    def mkdir(self, dir_path, job_name=None, log=False, wait=False, **kwargs):
        # Makes a directory if it doesn't already exists
        cmd_generator = StorageHelper.__get_storage_cmd_generator(dir_path)
        cmd = cmd_generator.mkdir(dir_path)

        job_name = f"mkdir_{CloudPlatform.generate_unique_id()}" if job_name is None else job_name

        # Optionally add logging
        cmd = f"{cmd} !LOG3!" if log else cmd

        # Run command and return job namemk
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    def path_exists(self, path, job_name=None, **kwargs):

        # Ignore local paths
        if self.__get_file_protocol(path) == "Local":
            logging.warning(f"Ignoring path '{path}' as it is local on the disk image. Assuming the path is present!")
            return True

        try:

            # Check if path is prefix, and create StoragePrefix object and check if exists
            if path.endswith("*"):
                return StoragePrefix(path.rstrip("*")).exists()

            # Check if it exists as a file or folder, by creating StorageFile and StorageFolder object
            return StorageFile(path).exists() or StorageFolder(path).exists()

        except RuntimeError as e:
            if str(e) != "":
                logging.error(f"StorageHelper error for {job_name}:\n{e}")
            return False
        except:
            logging.error(f"Unable to check path existence: {path}")
            raise

    def get_file_size(self, path, job_name=None, **kwargs):

        # Ignore local paths
        if self.__get_file_protocol(path) == "Local":
            logging.warning(f"Ignoring path '{path}' as it is local on the disk image. Assuming the path is present!")
            return True

        try:
            # Generate StorageFile object
            _file = StorageFile(path)

            # Obtain size in bytes
            _size = _file.size

            # Convert to GB
            return float(_size)/2**30

        except BaseException as e:
            logging.error(f"Unable to get file size: {path}")
            if str(e) != "":
                logging.error(f"Received the following msg:\n{e}")
            raise

    def rm(self, path, job_name=None, log=True, wait=False, **kwargs):
        # Delete file from file system
        # Log the transfer unless otherwise specified
        cmd_generator = StorageHelper.__get_storage_cmd_generator(path)
        cmd = cmd_generator.rm(path)

        job_name = f"rm_{CloudPlatform.generate_unique_id()}" if job_name is None else job_name

        # Optionally add logging
        cmd = f"{cmd} !LOG3!" if log else cmd

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    @staticmethod
    def __get_storage_cmd_generator(src_path, dest_path=None):
        # Determine the class of file handler to use base on input file protocol types

        # Get file storage protocol for src, dest files
        protocols = [StorageHelper.__get_file_protocol(src_path)]
        if dest_path is not None:
            protocols.append(StorageHelper.__get_file_protocol(dest_path))

        # Remove 'Local' protocol
        while "Local" in protocols:
            protocols.remove("Local")

        # If no other protocols remain then use local storage handler
        if len(protocols) == 0:
            return LocalStorageCmdGenerator

        # Cycle through file handlers to see which ones satisfy file protocol type required

        # Get available storage handlers
        storage_handlers = StorageCmdGenerator.__subclasses__()
        for storage_handler in storage_handlers:
            if storage_handler.PROTOCOL.lower() in protocols:
                return storage_handler

        # Raise error because we can't handle the type of file currently
        logging.error("StorageHelper cannot handle one or more input file storage types!")
        logging.error(f"Path: {src_path}")
        if dest_path is not None:
            logging.error(f"Dest_path: {dest_path}")
        raise InvalidStorageTypeError("Cannot handle input file storage type!")

    @staticmethod
    def __get_file_protocol(path):
        if ":" not in path:
            return "Local"
        return path.split(":")[0]

    @staticmethod
    def get_base_filename(path):
        return path.rstrip("/").split("/")[-1]


class StorageCmdGenerator(object):
    PROTOCOL = None


class LocalStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "Local"

    @staticmethod
    def mv(src_path, dest_dir):
        # Move a file from one directory to another
        return f"sudo mv {src_path} {dest_dir}"

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return f"sudo mkdir -p {dir_path}"

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return f"sudo du -sh --apparent-size --bytes {path}"

    @staticmethod
    def ls(path):
        return f"sudo ls {path}"

    @staticmethod
    def rm(path):
        # Dear god do not give sudo privileges to this command
        return f"rm -rf {path}"


class GoogleStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "gs"

    @staticmethod
    def mv(src_path, dest_dir):
        # Move a file from one directory to another
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        return f"sudo gsutil {options_fast} cp -r {src_path} {dest_dir}"

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return f"touch dummy.txt ; gsutil cp dummy.txt {dir_path}; gsutil rm {dir_path}dummy.txt"

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return f"gsutil du -s {path}"

    @staticmethod
    def ls(path):
        return f"gsutil ls {path}"

    @staticmethod
    def rm(path):
        return f"gsutil rm -r {path}"


class AmazonStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "s3"

    @staticmethod
    def mv(src_path, dest_dir):

        # Reformat source path
        recursive_flag = '--recursive' if "/*" in src_path else ''
        src_path = src_path.rstrip("/*")
        dest_dir = dest_dir.rstrip("/*")

        return f"aws s3 cp {recursive_flag} {src_path} {dest_dir}/{os.path.basename(src_path)}"

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return f"touch dummy.txt ; aws s3 cp dummy.txt {dir_path}; aws s3 rm {dir_path}dummy.txt"

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return f"aws s3 ls {path} --recursive --summarize | tail -n1 | cut -d' ' -f6"

    @staticmethod
    def ls(path):
        return f"aws s3 ls {path}"

    @staticmethod
    def rm(path):
        return f"aws s3 rm --recursive {path}"

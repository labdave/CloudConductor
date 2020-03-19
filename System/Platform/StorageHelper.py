import logging
import os

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

        job_name = "mv_%s" % CloudPlatform.generate_unique_id() if job_name is None else job_name

        # Optionally add logging
        cmd = "%s !LOG3!" % cmd if log else cmd

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    def mkdir(self, dir_path, job_name=None, log=False, wait=False, **kwargs):
        # Makes a directory if it doesn't already exists
        cmd_generator = StorageHelper.__get_storage_cmd_generator(dir_path)
        cmd = cmd_generator.mkdir(dir_path)

        job_name = "mkdir_%s" % CloudPlatform.generate_unique_id() if job_name is None else job_name

        # Optionally add logging
        cmd = "%s !LOG3!" % cmd if log else cmd

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    def path_exists(self, path, job_name=None, **kwargs):
        # Return true if file exists, false otherwise
        cmd_generator = StorageHelper.__get_storage_cmd_generator(path)
        cmd = cmd_generator.ls(path)

        # Run command and return job name
        job_name = "check_exists_%s" % CloudPlatform.generate_unique_id() if job_name is None else job_name
        self.proc.run(job_name, cmd, **kwargs)

        # Wait for cmd to finish and get output
        try:
            self.proc.wait_process(job_name)
            return True
        except RuntimeError as e:
            if str(e) != "":
                logging.debug("StorageHelper error for %s:\n%s" % (job_name, e))
            return False
        except:
            logging.error("Unable to check path existence: %s" % path)
            raise

    def get_file_size(self, path, job_name=None, **kwargs):
        # Return file size in gigabytes
        cmd_generator = StorageHelper.__get_storage_cmd_generator(path)
        cmd = cmd_generator.get_file_size(path)

        # Run command and return job name
        job_name = "get_size_%s" % CloudPlatform.generate_unique_id() if job_name is None else job_name
        self.proc.run(job_name, cmd, **kwargs)

        # Wait for cmd to finish and get output
        try:
            # Try to return file size in gigabytes
            out, err = self.proc.wait_process(job_name)
            # Iterate over all files if multiple files (can happen if wildcard)
            bytes = [int(x.split()[0]) for x in out.split("\n") if x != ""]
            # Add them up and divide by billion bytes
            return sum(bytes)/(1024**3.0)

        except BaseException as e:
            logging.error("Unable to get file size: %s" % path)
            if str(e) != "":
                logging.error("Received the following msg:\n%s" % e)
            raise

    def rm(self, path, job_name=None, log=True, wait=False, **kwargs):
        # Delete file from file system
        # Log the transfer unless otherwise specified
        cmd_generator = StorageHelper.__get_storage_cmd_generator(path)
        cmd = cmd_generator.rm(path)

        job_name = "rm_%s" % CloudPlatform.generate_unique_id() if job_name is None else job_name

        # Optionally add logging
        cmd = "%s !LOG3!" % cmd if log else cmd

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
        logging.error("Path: %s" % src_path)
        if dest_path is not None:
            logging.error("Dest_path: %s" % dest_path)
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
        return "sudo mv %s %s" % (src_path, dest_dir)

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return "sudo mkdir -p %s" % dir_path

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return "sudo du -sh --apparent-size --bytes %s" % path

    @staticmethod
    def ls(path):
        return "sudo ls %s" % path

    @staticmethod
    def rm(path):
        # Dear god do not give sudo privileges to this command
        return "rm -rf %s" % path


class GoogleStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "gs"

    @staticmethod
    def mv(src_path, dest_dir):
        # Move a file from one directory to another
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        return "sudo gsutil %s cp -r %s %s" % (options_fast, src_path, dest_dir)

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return "touch dummy.txt ; gsutil cp dummy.txt %s" % dir_path

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return "gsutil du -s %s" % path

    @staticmethod
    def ls(path):
        return "gsutil ls %s" % path

    @staticmethod
    def rm(path):
        return "gsutil rm -r %s" % path


class AmazonStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "s3"

    @staticmethod
    def mv(src_path, dest_dir):

        # Reformat source path
        recursive_flag = '--recursive' if "/*" in src_path else ''
        src_path = src_path.rstrip("/*")
        dest_dir = dest_dir.rstrip("/*")

        return "aws s3 cp %s %s %s/%s" % \
               (recursive_flag, src_path, dest_dir, os.path.basename(src_path))

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return "touch dummy.txt ; aws s3 cp dummy.txt %s" % dir_path

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return "aws s3 ls %s --recursive --summarize | tail -n1 | cut -d' ' -f6" % path

    @staticmethod
    def ls(path):
        return "aws s3 ls %s" % path

    @staticmethod
    def rm(path):
        return "aws s3 rm --recursive %s" % path

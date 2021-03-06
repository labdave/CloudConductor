import logging
import os
import time

from System.Platform import Platform
import traceback
from Aries.storage import StorageFile, StoragePrefix, StorageFolder


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

        job_name = f"mv_{Platform.generate_unique_id()}" if job_name is None else job_name

        # Optionally add logging
        cmd = f"{cmd} !LOG3!" if log else cmd

        # Add correct docker image and entrypoint if not local
        if cmd_generator.PROTOCOL != "Local":
            kwargs["docker_image"] = "rclone/rclone:1.52"
            kwargs["docker_entrypoint"] = "rclone"

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        if wait:
            self.proc.wait_process(job_name)
        return job_name

    def mkdir(self, dir_path, job_name=None, log=False, wait=False, **kwargs):
        # Makes a directory if it doesn't already exists
        cmd_generator = StorageHelper.__get_storage_cmd_generator(dir_path)
        cmd = cmd_generator.mkdir(dir_path)

        if cmd is None:
            return None

        job_name = f"mkdir_{Platform.generate_unique_id()}" if job_name is None else job_name

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
            logging.debug(f"Checking existence of {path}...")
            # Check if path is prefix, and create StoragePrefix object and check if exists
            if path.endswith("*"):
                return StoragePrefix(path.rstrip("*")).exists()

            # Check if it exists as a file or folder, by creating StorageFile and StorageFolder object
            return StorageFile(path).exists() or StorageFolder(path).exists()

        except RuntimeError as e:
            traceback.print_exc()
            if str(e) != "":
                logging.error(f"StorageHelper error for {job_name}:\n{e}")
            return False
        except:
            traceback.print_exc()
            logging.error(f"Unable to check path existence: {path}")
            raise

    def get_file_size(self, path, job_name=None, **kwargs):

        retry_count = kwargs.get("retry_count", 0)

        # Ignore local paths
        if self.__get_file_protocol(path) == "Local":
            logging.warning(f"Ignoring path '{path}' as it is local on the disk image. Assuming the path is present!")
            return True

        if retry_count < 5:
            try:
                # Check if path is prefix, and create StoragePrefix object and get its size
                if path.endswith("*"):
                    _size = StoragePrefix(path.rstrip("*")).size

                # Check if it path exists as a file or folder, by creating StorageFile and StorageFolder object
                else:
                    _file = StorageFile(path)
                    _folder = StorageFolder(path)
                    _size = 0

                    found = False
                    trial_count = 0
                    while not found:

                        if trial_count > 10:
                            logging.error(f"Cannot get size of '{path}' as it doesn't exist after multiple trials!")
                            break

                        time.sleep(trial_count)

                        if _file.exists():
                            _size = _file.size
                            found = True
                        elif _folder.exists():
                            _size = _folder.size
                            found = True
                        else:
                            trial_count += 1
                            logging.warning(f"Cannot get size of '{path}' as it does not exist! Trial {trial_count}/10")

                # Convert to GB
                return float(_size)/2**30

            except BaseException as e:
                logging.error(f"Unable to get file size: {path}")
                if str(e) != "":
                    logging.error(f"Received the following msg:\n{e}")
                if "dictionary changed size" in str(e):
                    kwargs['retry_count'] = retry_count + 1
                    return self.get_file_size(path, job_name, **kwargs)
                raise
        else:
            logging.warning(f"Failed to get size of '{path}'! Attempted to retrieve size {retry_count + 1} times.")
            return 0

    def rm(self, path, job_name=None, log=True, wait=False, **kwargs):
        # Delete file from file system
        # Log the transfer unless otherwise specified

        # Create prefix object
        _prefix_path = StoragePrefix(path)

        try:

            if _prefix_path.exists():
                _prefix_path.delete()

        except:
            logging.error(f"Unable to delete path: {path}")
            raise

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
    def rm(path):
        # Dear god do not give sudo privileges to this command
        return f"rm -rf {path}"


class GoogleStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "gs"

    @staticmethod
    def mv(src_path, dest_dir):

        # Check if it is remote directory
        is_directory = StorageFolder(src_path).exists()

        # Convert to Rclone remote structure
        src_path = src_path.replace("gs://", "gs:")
        dest_dir = dest_dir.replace("gs://", "gs:")

        if dest_dir.endswith("/"):
            cmd = "copy"
        else:
            cmd = "copyto"

        if src_path.endswith("*"):
            basedir, basename = src_path.rsplit("/", 1)
            return f"--include {basename} {cmd} {basedir} {dest_dir} --progress"
        elif src_path.startswith("gs:"):
            if is_directory:
                newdir = src_path.rstrip("/").rsplit("/", 1)[-1]
                return f"{cmd} {src_path} {dest_dir}{newdir} --progress"
            else:
                return f"{cmd} {src_path} {dest_dir}"
        else:
            newdir = src_path.rstrip("/").rsplit("/", 1)[-1]
            return f"{cmd} {src_path} {dest_dir}$([ -d {src_path} ] && echo '/{newdir}') --progress"

    @staticmethod
    def mkdir(dir_path):
        # Skip making directory as Google Storage doesn't have concept of directories
        return None

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return f"gsutil du -s {path}"

    @staticmethod
    def rm(path):
        return f"gsutil rm -r {path}"


class AmazonStorageCmdGenerator(StorageCmdGenerator):

    PROTOCOL = "s3"

    @staticmethod
    def mv(src_path, dest_dir):

        # Check if it is remote directory
        is_directory = StorageFolder(src_path).exists()

        # Convert to Rclone remote structure
        src_path = src_path.replace("s3://", "s3:")
        dest_dir = dest_dir.replace("s3://", "s3:")

        if dest_dir.endswith("/"):
            cmd = "copy"
        else:
            cmd = "copyto"

        if src_path.endswith("*"):
            basedir, basename = src_path.rsplit("/", 1)
            return f"--include {basename} {cmd} {basedir} {dest_dir} --progress"
        elif src_path.startswith("s3:"):
            if is_directory:
                newdir = src_path.rstrip("/").rsplit("/", 1)[-1]
                return f"{cmd} {src_path} {dest_dir}/{newdir} --progress"
            else:
                return f"{cmd} {src_path} {dest_dir} --progress"
        else:
            newdir = src_path.rstrip("/").rsplit("/", 1)[-1]
            return f"{cmd} {src_path} {dest_dir}$([ -d {src_path} ] && echo '/{newdir}') --progress"

    @staticmethod
    def mkdir(dir_path):
        # Skip making directory as Amazon Storage doesn't have concept of directories
        return None

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return f"gsutil du -s {path}"

    @staticmethod
    def rm(path):
        return f"gsutil rm -r {path}"

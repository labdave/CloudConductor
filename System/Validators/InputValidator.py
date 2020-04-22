import logging

from .Validator import Validator
from System.Workers import ThreadPool, PoolWorker
from System.Datastore import GAPFile
from System.Platform import StorageHelper, DockerHelper

class InputValidator(Validator):

    def __init__(self, resource_kit, sample_data, storage_helper, docker_helper, num_threads=25):
        super(InputValidator, self).__init__()
        # Check whether all input files declared in resource kit and sample data exist
        self.resources  = resource_kit
        self.samples    = sample_data
        self.storage_helper = storage_helper
        self.docker_helper  = docker_helper

        # Create thread pool for parallelizing input file validation
        self.thread_pool = ThreadPool(num_threads, worker_class=InputWorker, storage_helper=self.storage_helper, docker_helper=self.docker_helper)

    def validate(self):

        # Check resource kit paths
        inputs = {}
        inputs["resource"] = self.__get_resource_paths()

        # Check resource kit docker images
        inputs["docker"] = self.__get_docker_images()

        # Check sample data paths
        inputs["sample"] = self.__get_sample_data_paths()

        # Validate all files by adding them to thread pool's queue
        for input_file_src in inputs:
            for input_file in inputs[input_file_src]:
                input_desc = self.__get_input_desc(input_file, input_source=input_file_src)
                logging.info("Validating %s..." % input_desc)
                self.thread_pool.add_task(input_file, input_desc)

        # Wait for all tasks to finish
        self.thread_pool.wait_completion()

        # Run through all files and see if they've been validated
        for input_file_src in inputs:
            for input_file in inputs[input_file_src]:
                input_desc = self.__get_input_desc(input_file, input_source=input_file_src)

                logging.info("Input: %s.\nvalidated: %s\nvalidation_failed: %s\nmissing: %s\nsize: %sGB\n\n" % (input_desc,
                                                                                             input_file.is_flagged("validated"),
                                                                                             input_file.is_flagged("validation_failed"),
                                                                                             input_file.is_flagged("missing"),
                                                                                             input_file.get_size()))

                # Only report on files that were actually validated
                # Some may not have been validated if thread pool closed due to terminal error
                if not input_file.is_flagged("validated"):
                    continue

                # Report error if validation failed due to error other than non-existence or
                elif input_file.is_flagged("validation_failed"):
                    self.report_error("%s is invalid resource file! Check error log for details." % input_desc)

                # Report errr if validation failed because file/docker doesn't exist
                elif input_file.is_flagged("missing"):
                    self.report_error("%s does not exist!" % input_desc)

                elif input_file.get_size() is None:
                    self.report_error("Could not determine size of %s!" % input_desc)

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors

    @staticmethod
    def __get_input_desc(input_obj, input_source):
        # Return an informative description about an input
        if isinstance(input_obj, GAPFile):
            return "%s file '%s' of type '%s' with path %s" % \
                   (input_source, input_obj.get_file_id(), input_obj.get_type(), input_obj.get_path())
        else:
            return "Docker '%s' with image %s" % (input_obj.get_ID(), input_obj.get_image_name())

    def __get_resource_paths(self):
        # Check whether all paths in resource kit exist
        # Obtain the resource paths
        paths = []
        res = self.resources.get_resources()
        # Check if each resource path is present
        for res_type in res:
            for res_name in res[res_type]:
                res_obj = res[res_type][res_name]
                paths.append(res_obj)
        return paths

    def __get_docker_images(self):
        # Return list of docker images in resource kit
        return list(self.resources.get_docker_images().values())

    def __get_sample_data_paths(self):
        # Return list of paths in sample data
        paths = []
        sample_paths = self.samples.get_paths()
        # Check if the path exists
        for input_type, sample_paths in sample_paths.items():
            if isinstance(sample_paths, list):
                for path in sample_paths:
                    paths.append(path)
            else:
                paths.append(sample_paths)
        return paths


class InputWorker(PoolWorker):
    # ThreadPool worker for determining whether a single input (docker images/files, etc.) exists
    def __init__(self, task_queue, storage_helper=None, docker_helper=None):

        # Docker and storage helpers used to check existence of inputs
        self.storage_helper = storage_helper
        self.docker_helper  = docker_helper

        # Check to make sure they're the correct class
        assert isinstance(storage_helper, StorageHelper), "InputWorker needs valid StorageHelper class upon instantiation!"
        assert isinstance(docker_helper, DockerHelper), "InputWorker needs valid DockerHelper class upon instantiation!"

        # Start running task worker
        super(InputWorker, self).__init__(task_queue)

    def task(self, input_obj, input_desc):

        # Reset object size, existence attributes
        input_obj.flag("validated")
        input_obj.set_size(None)
        input_obj.flag("missing")
        input_obj.unflag("validation_failed")

        try:
            # Validate File object (Input type is meant to be ResourceKit, SampleSheet, etc.)
            if isinstance(input_obj, GAPFile):
                # input_name = "%s file '%s' of type '%s' with path: %s" % (input_type, input_obj.get_file_id(), input_obj.get_type(), input_obj)
                self.validate_file(input_obj)

            # Validate DockerImage object
            else:
                # input_name = "Docker image %s" % input_obj.get_image_name()
                self.validate_docker_image(input_obj)

        except BaseException as e:
            # Raise error because a command failed for a reason other than a file not existing
            input_obj.flag("validation_failed")
            logging.error("Unable to validate %s!" % input_desc)
            raise

    def validate_file(self, input_obj):
        # Check whether input file exists
        path_to_check = input_obj.get_transferrable_path() if input_obj.is_prefix() else input_obj.get_path()

        # Unset missing flag if path exists
        job_name = "check_exists_%s" % input_obj.get_file_id()
        if self.storage_helper.path_exists(path_to_check, job_name=job_name):
            input_obj.unflag("missing")

            # Get/set size of input file
            job_name = "get_size_%s" % input_obj.get_file_id()
            file_size = self.storage_helper.get_file_size(input_obj.get_transferrable_path(), job_name=job_name)
            input_obj.set_size(file_size)

    def validate_docker_image(self, docker_obj):
        # Check whether Docker image exists
        image_name = docker_obj.get_image_name()

        # Unset missing flag if Docker image exists
        job_name = "check_exists_%s" % docker_obj.get_ID()
        if self.docker_helper.image_exists(image_name, job_name=job_name):
            docker_obj.unflag("missing")

            # Get/set size of docker image
            job_name = "get_size_%s" % docker_obj.get_ID()
            image_size = self.docker_helper.get_image_size(image_name, job_name=job_name)
            docker_obj.set_size(image_size)

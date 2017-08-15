
from System.Validators import Validator

class PlatformValidator(Validator):

    def __init__(self, pipeline_obj):

        super(PlatformValidator, self).__init__(pipeline_obj)

    def __check_final_output_dir(self):

        final_output_dir = self.platform.get_final_output_dir()

        # Check if the final output directory has been created
        if not self.platform.path_exists(final_output_dir):

            self.report_error("The final output directory path ('%s') does not exist!" % final_output_dir)

    def validate_before_launch(self):

        self.__check_final_output_dir()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors

    def __check_workspace_dir(self):

        # Obtain all the workspace directories
        workspace_dirs = self.platform.define_workspace()

        # Define directory names
        dirs = {
            "wrk": "working",
            "bin": "binary",
            "tmp": "temporary",
            "lib": "library",
            "res": "resources",
        }

        for dir_name, dir_path in workspace_dirs.iteritems():
            if not self.platform.path_exists(dir_path):
                self.report_error("In platform, the workspace %s directory ('%s') is missing! "
                                  "Please create this directory "
                                  "in the init__workspace() method." % (dirs[dir_name],dir_path))

    def __check_resource_paths(self):

        for resource_type, resource_names in self.resources.get_resources():
            for resource_name, resource_obj in resource_names:

                # Obtain the resource object path
                path = resource_obj.get_path()

                # Check if the path exists
                if not self.platform.path_exists(path):
                    self.report_error("The resource '%s' of type '%s' has not been found on the platform. "
                                      "The resource path is %s. Please ensure the resource is defined correctly."
                                      % (resource_name, resource_type, path))

    def __check_input_data_paths(self):

        # Obtain sample data and paths
        sample_data  = self.samples.get_data()
        sample_paths = self.samples.get_paths()

        # Check the existance of each sample path
        for path_name, paths in sample_paths:

            if isinstance(paths, list):
                for sample_name, path in zip(sample_data["sample_name"], paths):
                    if not self.platform.path_exists(path):
                        self.report_error("For sample '%s', the input data '%s' with path '%s' has not been "
                                          "found on the platform."
                                          "Please ensure the the input data is defined correctly."
                                          % (sample_name, path_name, path))

            else:
                if not self.platform.path_exists(paths):
                    self.report_error("For sample '%s', the input data '%s' with path '%s' has not been "
                                      "found on the platform."
                                      "Please ensure the the input data is defined correctly."
                                      % (sample_data["sample_name"], path_name, paths))

    def validate_after_launch(self):

        # Check if workspace dir is completely initialized
        self.__check_workspace_dir()

        # Check if all the paths are loaded on the platform
        self.__check_resource_paths()
        self.__check_input_data_paths()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors

import copy
import os
import logging

from System.Datastore import GAPFile
from System.Platform import Platform

class PrematureTaskInputSetError(Exception):
    pass

class Datastore(object):
    # Object containing all available information to GAP modules at any given instance
    def __init__(self, graph, resource_kit, sample_data, platform):

        self.graph = graph
        self.resource_kit = resource_kit
        self.sample_data = sample_data
        self.platform = platform

        # Base directories for task execution (wrk) and output storage (output)
        self.__base_wrk_dir = self.platform.wrk_dir
        self.__base_output_dir = self.platform.final_output_dir

    def set_task_input_args(self, task_id):
        # Set input arguments for a task module

        # Throw error if task inputs aren't ready to be set
        if not self.graph.parents_complete(task_id):
            logging.error("Cannot set arguments for task '%s' before upstream tasks have completed!" % task_id)
            raise PrematureTaskInputSetError("Cannot set task arguments before a task dependencies have completed!")

        task_module = self.graph.get_tasks(task_id).module
        for input_type, input_arg in task_module.get_arguments().items():
            logging.debug("(%s) Setting arg: %s" % (task_id, input_type))
            val = self.__get_task_arg(task_id, input_type, is_resource=input_arg.is_resource())
            if val is None:
                val = input_arg.get_default_value()
            task_module.set_argument(input_type, val)
            logging.debug("(%s) Arg type: %s, val: %s" % (task_id, input_type, val))

        # Re-format nr_cpus, mem
        nr_cpus     = self.__reformat_nr_cpus(task_module)
        mem         = self.__reformat_mem(task_module.get_argument("mem"), nr_cpus)
        logging.debug("(%s) Reformatted arg type: nr_cpus, val: %s" % (task_id, nr_cpus))
        logging.debug("(%s) Reformatted arg type: mem, val: %s" % (task_id, mem))

        # Reset nr_cpus, mem
        task_module.set_argument("nr_cpus", nr_cpus)
        task_module.set_argument("mem", mem)

    def get_task_workspace(self, task_id=None):
        # Use task information to generate unique directories for input/output files

        if task_id is None:
            wrk_dir = self.__base_wrk_dir
            tmp_output_dir = os.path.join(self.__base_output_dir, "tmp")
            wrk_output_dir = os.path.join(wrk_dir, "output")
            final_output_dir = self.__base_output_dir

        else:
            task = self.graph.get_tasks(task_id)
            visible_samples = task.get_visible_samples()

            # Create subfolders for split tasks
            if task.is_split():
                task_id = task_id.replace(".", "/")

            wrk_dir = os.path.join(self.__base_wrk_dir, task_id)
            tmp_output_dir = os.path.join(self.__base_output_dir,"tmp", task_id)
            wrk_output_dir = os.path.join(wrk_dir, "output")
            final_output_dir = os.path.join(self.__base_output_dir, task_id)

            if visible_samples is not None and len(visible_samples) <= 1 and self.sample_data.get_num_samples() <=1:
                # Single sample output of multi-sample analysis always goes in sample level folder
                sample_name = visible_samples[0]
                # Remove sample name from path if it already appears and put it as the first directory
                task_id = task_id.replace(sample_name+"/", "")
                final_output_dir = os.path.join(self.__base_output_dir, sample_name, task_id)

        # Standardize directories
        wrk_dir             = self.platform.standardize_dir(wrk_dir)
        tmp_output_dir      = self.platform.standardize_dir(tmp_output_dir)
        wrk_output_dir      = self.platform.standardize_dir(wrk_output_dir)
        final_output_dir    = self.platform.standardize_dir(final_output_dir)

        # Create and return TaskWorkspace
        return TaskWorkspace(wrk_dir, tmp_output_dir, wrk_output_dir, final_output_dir)

    def get_docker_image(self, docker_id):
        return self.resource_kit.get_docker_images(docker_id)

    def get_task_input_files(self, task_id):
        # Return list of input files that need to be loaded for in order for task to run

        # Get nested list of module arguments
        module = self.graph.get_tasks(task_id).get_module()
        inputs = module.get_input_values()

        # Flatten nested list into a single list
        inputs = flatten(inputs)

        # Loop through and determine which are files
        input_files = []
        for input_file in inputs:
            # Append input if it's a file and one that doesn't appear on the docker
            if isinstance(input_file, GAPFile) and not input_file.is_flagged("docker"):
                input_files.append(input_file)

        return input_files

    def get_task_output_files(self, task_id):
        # Return list of output files produced by task
        module = self.graph.get_tasks(task_id).get_module()
        outputs = module.get_output_values()

        # Flatten nested list into a single list
        outputs = flatten(outputs)

        # Loop through and determine which are files
        output_files = []
        for output in outputs:
            # Append if output is an instance of
            if isinstance(output, GAPFile):
                output_files.append(output)

        # Return actual copies so that module paths get updated as they are transferred
        return output_files

    def __get_task_arg(self, task_id, arg_type, is_resource=False):
        # Return the object that best satisfies the arg_type for a task

        # Get all objects visible to task that match the arg_type
        possible_args = self.__gather_args(task_id, arg_type)

        # Select the object using precedence rules
        final_arg = self.__select_arg(possible_args, is_resource=is_resource)

        # Make deep copy of argument so internal datastore values can't be touched
        final_arg = copy.deepcopy(final_arg) if final_arg is not None else final_arg

        return final_arg

    def __select_arg(self, avail_args, is_resource=False):
        # Priority of checking for argument
        if not is_resource:
            input_order = ["parent_input", "docker_input", "resource_input", "sample_input", "config_input"]

        # Special case when argument MUST be a resource type. In this case we can only get the arg from the RK.
        else:
            input_order = ["docker_input", "resource_input", "parent_input", "sample_input", "config_input"]

        # Search the key in each input type
        for input_type in input_order:
            # List of values matching type
            if len(avail_args[input_type]) > 1:
                return avail_args[input_type]
            # Single value matching type
            if len(avail_args[input_type]) > 0:
                try:
                    return avail_args[input_type][0]
                except KeyError:
                    logging.error("Input type %s has no available value." % input_type)
                    return
        return None

    def __gather_args(self, task_id, arg_type):
        # Gather possible inputs to a task matching arg_type
        possible = {}

        # Get args from parent tasks
        possible["parent_input"] = self.__gather_parent_args(task_id, arg_type)

        # Get args from docker requested by task
        possible["docker_input"] = self.__gather_docker_args(task_id, arg_type)

        # Get args from resource kit
        possible["resource_input"] = self.__gather_res_kit_args(task_id, arg_type)

        # Get args from sample data
        possible["sample_input"] = self.__gather_sample_args(task_id, arg_type)

        # Get args from config input
        config_input = self.graph.get_tasks(task_id).get_graph_config_args()
        possible["config_input"] = [] if arg_type not in config_input else config_input[arg_type]

        return possible

    def __gather_parent_args(self, task_id, arg_type):
        # Get args of specified type inherited from parent tasks
        args = []
        curr_task = self.graph.get_tasks(task_id)
        for parent_id in self.graph.get_parents(task_id):
            parent = self.graph.get_tasks(parent_id)
            if parent.is_splitter_task():
                # Limit output to partition visible to task
                split_id = curr_task.get_split_id()
                output = parent.module.get_output(split_id=split_id)
            else:
                output = parent.module.get_output()
            if arg_type in output:
                args.append(output[arg_type])
        return args

    def __gather_sample_args(self, task_id, arg_type):
        # Get args of specified type from sample data
        args = []
        # Get list of samples visible to current task
        curr_task = self.graph.get_tasks(task_id)
        visible_samples = curr_task.get_visible_samples()
        if self.sample_data.has_data_type(arg_type):

            # Restrict sample sheet access to visible samples if necessary
            if visible_samples is not None:
                args = self.sample_data.get_data(arg_type, samples=visible_samples)

            # Return variables from all samples
            else:
                args = self.sample_data.get_data(arg_type)

        # Coerce to list if not one already
        if not isinstance(args, list):
            args = [args]

        return args

    def __gather_res_kit_args(self, task_id, arg_type):

        args = []

        if self.resource_kit.has_resource_type(arg_type):

            # Search to see if the argument key appears in the config input
            config_input = self.graph.get_tasks(task_id).get_graph_config_args()
            if arg_type in config_input:
                # Obtain the resource name
                res_name = config_input[arg_type]

                if res_name in self.resource_kit.get_resources(arg_type):

                    # Get the resource object with name "res_name" from resource input data
                    args = [self.resource_kit.get_resources(arg_type)[res_name]]

            # If not in config input, should only be one resource of type "arg_key"
            else:
                # Only return the resource of type "arg_key" if it is one value
                if len(self.resource_kit.get_resources(arg_type)) == 1:
                    args = [list(self.resource_kit.get_resources(arg_type).values())[0]]

        return args

    def __gather_docker_args(self, task_id, arg_type):

        args = []
        docker_image_id = self.graph.get_tasks(task_id).get_docker_image_id()

        if docker_image_id is not None and self.resource_kit.has_docker_image(docker_image_id):
            docker_image = self.resource_kit.get_docker_images(docker_image_id)
            if docker_image.has_resource_type(arg_type):

                # Search to see if the argument key appears in the config input
                config_input = self.graph.get_tasks(task_id).get_graph_config_args()
                if arg_type in config_input:

                    res_name = config_input[arg_type]

                    if res_name in docker_image.get_resources(arg_type):
                        # Get the resource object with name "res_name" from docker input data
                        args = [docker_image.get_resources(arg_type)[res_name]]

                # If not in config input, should only be one resource of type "arg_key" in docker
                else:
                    args = [list(docker_image.get_resources(arg_type).values())[0]]

        return args

    def __reformat_nr_cpus(self, task_module):
        nr_cpus = task_module.get_argument("nr_cpus")

        # Makes sure the argument for nr_cpus is valid
        max_cpus = self.platform.get_max_nr_cpus()

        # CPUs = 'max' converted to platform maximum cpus
        if isinstance(nr_cpus, str):
            if nr_cpus.isdigit():
                nr_cpus = int(nr_cpus)
            elif nr_cpus.lower() == "max":
                # Set special case for maximum nr_cpus
                nr_cpus = max_cpus
            elif nr_cpus in task_module.arguments:
                # If nr_cpus is a str of input argument
                # Set nr_cpus to be the number of elements in the inputs
                inputs = task_module.get_argument(nr_cpus)
                # Set a minimum of 2 CPUs in this case to handle the possible overhead.
                if isinstance(inputs, list):
                    nr_cpus = max(len(inputs), 2)
                else:
                    nr_cpus = 2
            else:
                raise IOError("Number of CPUs defined incorrectly: %s" % nr_cpus)

        # CPUs > 'max' converted to maximum cpus
        if int(nr_cpus) > int(max_cpus):
            nr_cpus = max_cpus

        # Update module nr_cpus argument
        return int(nr_cpus)

    def __reformat_mem(self, mem, nr_cpus):
        max_mem = self.platform.get_max_mem()
        if isinstance(mem, str):
            # Special case where mem is platform max
            if mem.lower() == "max":
                mem = max_mem
            # Special case if memory is scales with nr_cpus (e.g. 'nr_cpus * 1.5')
            elif "nr_cpus" in mem.lower():
                mem_expr = mem.lower()
                mem = int(eval(mem_expr.replace("nr_cpus", str(nr_cpus))))
        # Set to platform max mem if over limit
        if int(mem) > int(max_mem):
            mem = max_mem
        # Update module memory argument
        return int(mem)

class TaskWorkspace(object):
    # Defines folder structure where task will execute/files generated
    def __init__(self, wrk_dir, tmp_output_dir, wrk_output, final_output_dir):
        # wrk_dir: Folder where input files will be generated by task
        # tmp_output_dir: Folder where temporary final output will be saved until all tasks are finished
        # wrk_output dir: Folder where output files will be generated by task
        # final_output_dir: Folder where final output files will be saved
        self.workspace = {"wrk" : wrk_dir,
                          "wrk_output": wrk_output,
                          "tmp_output" : tmp_output_dir,
                          "final_output" : final_output_dir}

        # Define wrk/final log directories
        self.workspace["wrk_log"] = os.path.join(wrk_dir, "log/")
        self.workspace["final_log"] = os.path.join(final_output_dir, "log/")

        # Standardize directory paths
        for dir_type, dir_path in self.workspace.items():
            dir_path = Platform.standardize_dir(dir_path)
            self.workspace[dir_type] = dir_path

    def get_wrk_dir(self):
        return self.workspace["wrk"]

    def get_wrk_out_dir(self):
        return self.workspace["wrk_output"]

    def get_output_dir(self):
        return self.workspace["final_output"]

    def get_tmp_output_dir(self):
        return self.workspace["tmp_output"]

    def get_wrk_log_dir(self):
        return self.workspace["wrk_log"]

    def get_final_log_dir(self):
        return self.workspace["final_log"]

    def get_workspace(self):
        return self.workspace

    def debug_string(self):
        to_return = "=============\n"
        to_return += "Wrk:\t%s\n" % self.get_wrk_dir()
        to_return += "Wrk Out:\t%s\n" % self.get_wrk_out_dir()
        to_return += "TmpOutput:\t%s\n" % self.get_tmp_output_dir()
        to_return += "FinalOutput:\t%s\n" % self.get_output_dir()
        to_return += "WrkLog:\t%s\n" % self.get_wrk_log_dir()
        to_return += "FinalLog:\t%s\n" % self.get_final_log_dir()
        to_return += "=============\n"
        return to_return


# Method for unpacking nested list taken from http://code.activestate.com/recipes/578948-flattening-an-arbitrarily-nested-list-in-python/
def flatten(lis):
    """Given a list, possibly nested to any level, return it flattened."""
    new_lis = []
    for item in lis:
        if type(item) == type([]):
            new_lis.extend(flatten(item))
        else:
            new_lis.append(item)
    return new_lis


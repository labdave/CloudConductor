"""Genetic module for running a command with docker image.

This module can be used to run a command with any docker image defined in the resource kit.
The motivation of having this module is to run simple docker commands without having to add new code to CloudConductor

The docker_image should be defined in the resource kit.

In the "args" of the graph config, the following arguments are required:
    inputs: a list of inputs to this module.
    outputs: a list of output variables defined as key-value pairs.
    commands: the command to the executed.
Additional "args" can be defined to be used as inputs to the module.

The "inputs" defined in the "args" can be used in the "commands" as environment variable,
    similar to the way we use linux variable in bash command.
The "inputs" can contain CloudConductor resources or variables defined in "args" of this module.
Input variable defined as a list will be joined by SPACE.
The "outputs" defined in the "args" are variables that will be available for downstream steps.
Input variables can also be used when defining the outputs (same as how you use them in bash command)
The "OUTPUT_PATH" variable stores the CC output directory obtained by self.get_output_dir().

Optionally, you can defined the cpus and memory in the args of the module.
By default the docker module uses 1 CPU and 4GB memory.

[demuxtna__tna]
    module = Docker
    docker_image = cancer
    final_output = demux_stats
    input_from = utils_consolidatesamplename__tna

    [[args]]
        cpus = 32
        memory = 16
        inputs = R1, R2, sample_name, barcodes
        barcodes = AGTCGACATG, TCGACATG
        commands = python -m Cancer.run demux_inline --r1 ${R1} --r2 ${R2} --barcode "${barcodes}=${OUTPUT_PATH}${sample_name}.RNA" --unmatched ${OUTPUT_PATH}${sample_name}.non-RNA --stats ${OUTPUT_PATH}all_barcode_stats.csv --name ${sample_name} --header rna
        [[[outputs]]]
            R1 = ${OUTPUT_PATH}${sample_name}.RNA.R1.fastq.gz, ${OUTPUT_PATH}${sample_name}.non-RNA.R1.fastq.gz
            R2 = ${OUTPUT_PATH}${sample_name}.RNA.R2.fastq.gz, ${OUTPUT_PATH}${sample_name}.non-RNA.R2.fastq.gz
            demux_stats = ${OUTPUT_PATH}all_barcode_stats.csv
            assay_type = rna, dna

"""
import logging
from Modules import Module
logger = logging.getLogger(__name__)


class Docker(Module):
    # Environment variable storing the output path.
    OUTPUT_PATH = "${OUTPUT_PATH}"

    def __init__(self, module_id, is_docker=True, module_args=None):
        super().__init__(module_id, is_docker, module_args=module_args)

        # Declare the outputs
        outputs = self.module_args.get("outputs", dict())
        if not isinstance(outputs, dict):
            raise ValueError("Docker module outputs must be key-value pairs. Invalid value: %s" % outputs)
        self.output_keys = outputs.keys()
        # logger.debug("Docker module output keys: %s" % self.output_keys)

    def get_input_list(self):
        """Gets a list of inputs as defined in the config args.

        Returns: A list of strings, each represents an input to the module.

        """
        # Use sample_id as input if there is no input.
        # Otherwise CloudConductor will fail.
        inputs = self.module_args.get("inputs", ["sample_id"])
        # logger.debug("Inputs: %s" % inputs)
        if inputs and not isinstance(inputs, list):
            inputs = [inputs]
        return inputs

    def get_var_dict(self):
        """Gets a dictionary of environment variables from the arguments of the module.
        Each environment variable will have the key like ${VARIABLE_NAME}.
        Each value will be a value returned by get_argument(), converted to a string.
        If the value is a list, it will be converted to space-separated string.
        Other value types will be converted to string by calling str().

        ${OUTPUT_PATH} will be added with the value of output directory path.
        """
        # Set output path as env variable
        var_dict = {self.OUTPUT_PATH: self.get_output_dir()}
        for arg in self.get_input_list():
            val = self.get_argument(arg)
            if isinstance(val, list):
                val = " ".join(val)
            var_dict["${%s}" % arg] = str(val)
        return var_dict

    @staticmethod
    def replace_var(s, var_dict):
        """Replaces a string with environment variable keys with the corresponding values

        Args:
            s: A string, which may contain variable represented by ${VARIABLE_NAME}
            var_dict (dict): A dictionary of environment variables

        """
        value = str(s)
        for var, val in var_dict.items():
            value = value.replace(var, val)
        return value

    def define_input(self):
        # inputs, outputs and command must be defined in the config args
        self.add_argument("inputs")
        self.add_argument("outputs")
        self.add_argument("commands")
        # cpus and memory are optional
        self.add_argument("cpus", is_required=False)
        self.add_argument("memory", is_required=False)

        # Declare inputs
        for arg in self.get_input_list():
            # logger.debug("Adding Docker Input: %s" % arg)
            # TODO: input is_required?
            self.add_argument(arg, is_required=True)

        # cpus and memory
        cpus = self.module_args.get("cpus", 1)
        mem = self.module_args.get("memory", 4)
        self.add_argument("nr_cpus", is_required=False, default_value=cpus)
        self.add_argument("mem", is_required=False, default_value=mem)

    def define_output(self):
        var_dict = self.get_var_dict()
        # Declare outputs
        outputs = self.module_args.get("outputs")
        # Do nothing if there is no output.
        if not outputs:
            return
        for key, value in outputs.items():
            # is_path will be True by default
            is_path = True
            if isinstance(value, list):
                values = value
                # is_path will be False if one of the values does not start with OUTPUT_PATH
                for v in values:
                    if not str(v).startswith(self.OUTPUT_PATH):
                        is_path = False
                output_value = [self.replace_var(v, var_dict) for v in values]
            else:
                if not str(value).startswith(self.OUTPUT_PATH):
                    is_path = False
                output_value = self.replace_var(value, var_dict)
            logger.debug("Adding Docker Output: %s - %s" % (key, output_value))
            self.add_output(key, output_value, is_path=is_path)

    def define_command(self):
        var_dict = self.get_var_dict()
        command = self.get_argument("commands")
        # logger.debug("Variables: %s" % var_dict)
        command = self.replace_var(command, var_dict)
        # Covert line break to "&&" and remove empty command
        command = " && ".join([c.strip() for c in command.split("\n") if c.strip()])
        # Group commands with {} and output logs for all commands.
        cmd = "{ %s ;} !LOG3!" % command
        return cmd

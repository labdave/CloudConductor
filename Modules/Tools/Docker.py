import os
import logging
from Modules import Module
logger = logging.getLogger(__name__)


class Docker(Module):
    def __init__(self, module_id, is_docker=True, module_args=None):
        logger.debug("Module Args: %s" % module_args)
        super().__init__(module_id, is_docker, module_args=module_args)
        outputs = self.module_args.get("outputs", dict())
        self.output_keys = outputs.keys()
        logger.debug("Output Keys: %s" % self.output_keys)

    def get_input_list(self):
        inputs = self.module_args.get("inputs", [])
        logger.debug("Inputs: %s" % inputs)
        if inputs and not isinstance(inputs, list):
            inputs = [inputs]
        return inputs

    def get_var_dict(self):
        var_dict = {"${OUTPUT}": self.get_output_dir()}
        for arg in self.get_input_list():
            val = self.get_argument(arg)
            if isinstance(val, list):
                val = " ".join(val)
            var_dict["${%s}" % arg] = val
        return var_dict

    @staticmethod
    def replace_var(s, var_dict):
        value = str(s)
        for var, val in var_dict.items():
            value = value.replace(var, val)
        return value

    def define_input(self):
        self.add_argument("inputs")
        self.add_argument("outputs")
        self.add_argument("command")
        self.add_argument("cpus", is_required=False)
        self.add_argument("memory", is_required=False)

        for arg in self.get_input_list():
            logger.debug("Adding Input: %s" % arg)
            self.add_argument(arg, is_required=False)

        cpus = self.module_args.get("cpus", 1)
        mem = self.module_args.get("memory", 4)

        self.add_argument("nr_cpus", is_required=False, default_value=cpus)
        self.add_argument("mem", is_required=False, default_value=mem)

    def define_output(self):
        var_dict = self.get_var_dict()
        outputs = self.module_args.get("outputs")
        for key, value in outputs.items():
            is_path = True
            if isinstance(value, list):
                values = [value]
                for v in values:
                    # is_path will be False if one of the values does not start with ${OUTPUT}
                    if not str(v).startswith("${OUTPUT}"):
                        is_path = False
                output_value = [self.replace_var(v, var_dict) for v in values]
            else:
                if not str(value).startswith("${OUTPUT}"):
                    is_path = False
                output_value = self.replace_var(value, var_dict)
            logger.debug("Adding Output: %s" % key)
            self.add_output(key, output_value, is_path=is_path)

    def define_command(self):
        var_dict = self.get_var_dict()
        command = self.get_argument("command")
        logger.debug("Variables: %s" % var_dict)
        command = self.replace_var(command, var_dict)
        # Covert line break to "&&" and remove empty command
        command = " && ".join([c.strip() for c in command.split("\n") if c.strip()])
        # Group commands with {} and output logs for all commands.
        cmd = "{ %s ;} !LOG3!" % command
        return cmd

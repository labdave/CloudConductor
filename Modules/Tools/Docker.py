import os
import logging
from Modules import Module
logger = logging.getLogger(__name__)


class Docker(Module):
    def __init__(self, module_id, is_docker=True):
        super().__init__(module_id, is_docker)
        outputs = self.get_argument("outputs")
        self.output_keys = outputs.keys()
        logger.debug("Output Keys: %s" % self.output_keys)

    def get_input_list(self):
        inputs = self.get_argument("inputs")
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
            value.replace(var, val)
        return value

    def define_input(self):
        self.add_argument("inputs")
        self.add_argument("outputs")
        self.add_argument("command")
        self.add_argument("cpus")
        self.add_argument("memory")
        self.add_argument("barcodes")
        logger.debug("B %s" % self.get_argument("barcodes"))

        for arg in self.get_input_list():
            logger.debug("Adding Input: %s" % arg)
            self.add_argument(arg, is_required=False)

        cpus = self.get_argument("cpus")
        if str(cpus).isdigit():
            nr_cpus = int(cpus)
        else:
            logger.debug("Getting #CPU from %s" % cpus)
            var = self.get_argument(cpus)
            logger.debug("%s: %s" % (cpus, var))
            nr_cpus = len(var) if var else 1

        mem = self.get_argument("memory")

        self.add_argument("nr_cpus", is_required=False, default_value=nr_cpus)
        self.add_argument("mem", is_required=False, default_value=mem)

    def define_output(self):
        var_dict = self.get_var_dict()
        outputs = self.get_argument("outputs")
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
        command = self.replace_var(command, var_dict)
        # Covert line break to "&&" and remove empty command
        command = " && ".join([c for c in command.split("\n") if c])

        # Group commands with {} and output logs for all commands.
        cmd = "{ %s ;} !LOG3!" % command
        return cmd

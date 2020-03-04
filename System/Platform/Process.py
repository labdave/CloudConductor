import logging

import subprocess as sp


class Process(sp.Popen):

    def __init__(self, *args, **kwargs):

        # Retrieve CloudConductor specific values
        self.command = kwargs.pop("original_cmd", True)
        self.num_retries = kwargs.pop("num_retries", 0)
        self.docker_image = kwargs.pop("docker_image", None)

        # Initialize process status
        self.complete = False
        self.to_rerun = False

        # Initialize output and err values
        self.out = ""
        self.err = ""

        super(Process, self).__init__(*args, **kwargs)

    def is_complete(self):
        return self.complete

    def wait_completion(self):

        # Return immediately if process has already been set to complete
        if self.complete:
            return

        # Wait for process to finish
        out, err = self.communicate()

        # Save output and error
        self.out = out.decode("utf8")
        self.err = err.decode("utf8")

        # Set process to complete
        self.complete = True

    def has_failed(self):

        # Obtain process return code
        ret_code = self.poll()

        # Check if failure
        return ret_code is not None and ret_code != 0

    def get_command(self):
        return self.command

    def get_num_retries(self):
        return self.num_retries

    def get_docker_image(self):
        return self.docker_image

    def get_output(self):
        return self.out, self.err

    def set_to_rerun(self):
        self.to_rerun = True

    def needs_rerun(self):
        return self.to_rerun

    @staticmethod
    def run_local_cmd(cmd, err_msg=None, num_retries=5, env_var=None, print_logs=False):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, env=env_var)
        out, err = proc.communicate()

        # Convert to string formats
        out = out.decode("utf8")
        err = err.decode("utf8")

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():

            # Retry command if possible
            if num_retries > 0:
                return Process.run_local_cmd(cmd, err_msg, num_retries=num_retries - 1)

            logging.error(f"Could not run the following local command:\n{cmd}")

            if err_msg is not None:
                logging.error(f"{err_msg}.\nThe following error appeared:\n    {err}")

            raise RuntimeError(err_msg)

        if print_logs:
            logging.info(f"OUTPUT FOR CMD:\n\n {cmd} \n\n {out}")
            logging.info(f"ERROR FOR CMD:\n\n {cmd} \n\n {err}")

        return out

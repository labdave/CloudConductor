import logging

class DockerHelper(object):
    # Class designed to facilitate remote file manipulations for a processor

    def __init__(self, proc):
        self.proc = proc

    def pull(self, image_name, job_name=None, log=True, **kwargs):
        # Pull docker image on local processor
        cmd = "sudo docker pull %s" % image_name

        job_name = "pull_%s" % image_name if job_name is None else job_name

        # Optionally add logging
        cmd = "%s !LOG3!" % cmd if log else cmd

        # Run command and return job name
        self.proc.run(job_name, cmd, **kwargs)
        return job_name

    def image_exists(self, image_name, job_name=None, **kwargs):
        # Return true if file exists, false otherwise

        # Run command and return job name
        job_name = "check_exists_%s" % image_name if job_name is None else job_name

        # Wait for cmd to finish and get output
        try:
            self.pull(image_name, job_name, log=False, **kwargs)
            self.proc.wait_process(job_name)
            return True
        except RuntimeError as e:
            if str(e) != "":
                logging.debug("DockerHelper error for %s:\n%s" % (job_name, e))
            return False
        except:
            logging.error("Unable to check docker image existence: %s" % image_name)
            raise

    def get_image_size(self, image_name, job_name=None, **kwargs):
        # Return file size in gigabytes
        cmd = "sudo docker image inspect %s --format='{{.Size}}'" % image_name

        # Run command and return job name
        job_name = "get_size_%s" % image_name if job_name is None else job_name
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
            logging.error("Unable to check docker image size: %s" % image_name)
            if str(e) != "":
                logging.error("Received the following msg:\n%s" % e)
            raise

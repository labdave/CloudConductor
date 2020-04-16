import logging
import subprocess as sp
import json
from .DockerImage import DockerImage


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
            result = self.get_docker_image_info(image_name)
            if result and 'id' in result:
                return True

            # Try using the Registry API
            # This API is less efficient than the DockerHub API as it makes an additional authorization request
            # This API works for GCR, but it is not tested for other registry.
            if DockerImage(image_name).is_accessible():
                return True

            # this should handle everything that doesn't exist on docker hub ( way less efficient )
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
        try:
            result = self.get_docker_image_info(image_name)
            if result and 'full_size' in result:
                # return the bytes converted to GB
                # the api returns the compressed size of the image so we'll multiply by 4 to be safe
                return int(result['full_size'])*4/(1024**3.0)

            # Size will be None if get_size() cannot be determine the image size.
            size = DockerImage(image_name).get_size()
            if size:
                return int(size)/(1024**3.0)

            # this should handle everything that doesn't exist on docker hub ( way less efficient )
            # Return file size in gigabytes
            cmd = "sudo docker image inspect %s --format='{{.Size}}'" % image_name

            # Run command and return job name
            job_name = "get_size_%s" % image_name if job_name is None else job_name
            self.proc.run(job_name, cmd, **kwargs)

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

    def get_docker_image_info(self, image_name):
        docker_image_split = image_name.rsplit(":", 1)
        image = docker_image_split[0]
        # Use the latest image if tag is not given
        tag = docker_image_split[1] if len(docker_image_split) > 1 else "latest"
        cmd = f'curl -s "https://hub.docker.com/v2/repositories/{image}/tags/{tag}" | jq .'

        proc = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Convert to string formats
        out = out.decode("utf8")
        err = err.decode("utf8")

        # Throw error if anything happened
        if len(err) != 0:
            return None

        # Return image info json
        return json.loads(out)

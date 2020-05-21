import datetime
import requests
import re
import logging
logger = logging.getLogger(__name__)


class DockerImage:
    """
    See Also:
        https://docs.docker.com/registry/spec/api/
        https://github.com/docker/distribution/blob/master/reference/reference.go
        https://docs.docker.com/engine/reference/commandline/pull/
    """

    DOCKER_IO_REGISTRY = "registry-1.docker.io"

    def __init__(self, name):
        self.name = name
        self.hostname, self.path, self.tag, self._digest = self.parse_name(self.name)
        self._manifest = None
        self._token = None
        self._token_expire_time = None

    @staticmethod
    def parse_name(name):
        """Parses docker image name, which can have the format as accepted by the docker pull command.

        Args:
            name (str): Docker image name, including tag or digest, i.e. NAME[:TAG|@DIGEST]

        Returns:
            A 4-tuple of hostname, path, tag and digest.
            hostname will have the default of "docker.io"
        """
        hostname, path = DockerImage.match_host(name)
        if not hostname:
            hostname = DockerImage.DOCKER_IO_REGISTRY

        # Try to match the digest followed by the image name.
        path, digest = DockerImage.match_digest(path)

        # There should be no tag if digest is followed by the image name.
        if digest:
            tag = None
        else:
            path, tag = DockerImage.match_tag(path)

        # Official images are identified with library/IMAGE_NAME
        if "/" not in path and hostname == DockerImage.DOCKER_IO_REGISTRY:
            path = "library/" + path

        return hostname, path, tag, digest

    @staticmethod
    def match_host(name):
        name_pattern = r"(?:((?:localhost)|(?:[^/]*\.[^/]*)|(?:[^/]*\:[^/]*))/)?(.+)"
        match = re.match(name_pattern, name)
        if not match:
            raise ValueError("Invalid image name: %s" % name)
        groups = match.groups()
        hostname = groups[0]
        path = groups[1]
        return hostname, path

    @staticmethod
    def match_tag(name):
        tag_pattern = r"(.+)\:([\w][\w.-]{0,127})"
        match = re.match(tag_pattern, name)
        if match:
            name = match.groups()[0]
            tag = match.groups()[1]
        else:
            tag = "latest"
        return name, tag

    @staticmethod
    def match_digest(name):
        digest_pattern = r"(.+)\@([^/]*)"
        match = re.match(digest_pattern, name)
        if match:
            name = match.groups()[0]
            digest = match.groups()[1]
        else:
            digest = None
        return name, digest

    @property
    def digest(self):
        if not self._digest:
            self._digest = self.get_manifest().headers.get("Docker-Content-Digest").strip()
        return self._digest

    @property
    def token(self):
        if self._token and self._token_expire_time and self._token_expire_time > datetime.datetime.now():
            return self._token
        self.get_token()
        return self._token

    def get_token(self):
        url = "https://auth.docker.io/token?scope=repository:%s:pull&service=registry.docker.io" % self.path
        r = requests.get(url).json()
        self._token = r.get("token")
        expire_sec = r.get("expires_in")
        if expire_sec and str(expire_sec).isdigit():
            self._token_expire_time = datetime.datetime.now() + datetime.timedelta(seconds=int(expire_sec))
        return self._token

    def send_get_request(self, url, headers=None):
        if headers is None:
            headers = {}
        # docker.io requires a token to access the API
        # Even though the token can be obtained without authorization
        if self.hostname == self.DOCKER_IO_REGISTRY and "Authorization" not in headers:
            headers["Authorization"] = "Bearer %s" % self.token
        response = requests.get(url, headers=headers)
        return response

    def get_manifest(self, digest=None):
        """Gets the manifest HTTP response.
        """
        if not self._manifest or digest:
            tag = self.tag if digest is None else digest
            url = "https://%s/v2/%s/manifests/%s" % (self.hostname, self.path, tag)
            response = self.send_get_request(
                url,
                headers={"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
            )
            if response.status_code != 200:
                raise ValueError(
                    "Request to %s failed with status code %s" % (url, response.status_code)
                )
            self._manifest = response
        return self._manifest

    def is_accessible(self):
        """Checks if the image is accessible.
        This method returns False if the existence cannot be determined.

        Returns:

        """
        try:
            self.get_manifest()
            return True
        except ValueError:
            return False

    def inspect(self):
        config_digest = self.get_manifest().json().get("config", {}).get("digest")
        url = "https://%s/v2/%s/blobs/%s" % (self.hostname, self.path, config_digest)
        return self.send_get_request(url).json()

    def get_size(self):
        """Gets the size of the docker image by adding the sizes of all layers.
        This method returns None if the size cannot be determined.
        """
        try:
            layers = self.get_manifest().json().get("layers")
            total_size = 0
            if layers:
                for layer in layers:
                    total_size += layer.get("size", 0)
        except Exception as ex:
            logger.error("Cannot determine image size for %s: %s" % (self.name, str(ex)))
            import traceback
            traceback.print_exc()
            return None
        return total_size

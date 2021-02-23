# Base Image
FROM ubuntu:18.04

# Metadata
LABEL base.image="CloudConductor:v0.1.1"
LABEL version="1"
LABEL software="CloudConductor:v0.1.1"
LABEL software.version="0.1.1"
LABEL description="Bioinformatics cloud workflow management system."
LABEL tags="NGS Cloud CloudConductor GoogleCloud AWS Bioinformatics Workflow Pipeline"

# Maintainer
LABEL Maintainer davelab  <lab.dave@gmail.com>

# update the OS related packages
RUN apt-get update && \
		apt-get install -y software-properties-common

RUN apt-get update -y &&\
    apt-get install -y build-essential python2.7-dev python3.6-dev python3-pip && \
    apt-get install -y curl git netcat jq

# Clone the repository
RUN git clone https://github.com/labdave/CloudConductor.git

# upgrade pip, setuptools, and wheel Python modules
RUN python3 -m pip install pip --upgrade && \
	python3 -m pip install setuptools wheel && \
    python3 -m pip install -r /CloudConductor/requirements.txt && \
    python3 -m pip uninstall -y Aries-storage Aries-core && \
    python3 -m pip install Aries-storage==0.1.318

# Install gcloud
RUN curl -sSL https://sdk.cloud.google.com > /tmp/gcl &&\
    bash /tmp/gcl --disable-prompts &&\
    echo "if [ -f '/root/google-cloud-sdk/path.bash.inc' ]; then source '/root/google-cloud-sdk/path.bash.inc'; fi" >> /root/.bashrc &&\
    echo "if [ -f '/root/google-cloud-sdk/completion.bash.inc' ]; then source '/root/google-cloud-sdk/completion.bash.inc'; fi" >> /root/.bashrc
ENV PATH /root/google-cloud-sdk/bin:$PATH

# Install gcloud beta components for pubsub
RUN /bin/bash -c "gcloud components install beta --quiet"

# Install aws cli
RUN python3 -m pip install awscli --upgrade

ENV PATH /CloudConductor:$PATH
ENV PATH /ModuleRunner:$PATH

CMD ["CloudConductor"]

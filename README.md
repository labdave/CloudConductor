<div align=center><img src="docs/_static/cloud-conductor-logo-colored.png" alt="CC" width=450 height=225 align="middle"/></div>

## CloudConductor: Simplified Bioinformatics

**CloudConductor** is a cloud-based workflow engine for defining and executing bioinformatics pipelines in a cloud environment. 
Currently, the framework has been tested extensively on the [Google Cloud Platform](https://cloud.google.com/), but will eventually support other platforms including AWS, Azure, etc.

## Feature Highlights

  * **User-friendly**
    * Define complex workflows by linking together user-defined modules that can be re-used across pipelines
    * [Config_obj](http://configobj.readthedocs.io/en/latest/configobj.html) for clean, readable workflows (see below example)
    * 50+ pre-installed modules for existing bioinformatics tools
  * **Portable**
    * Docker integration ensures reproducible runtime environment for modules    
    * Platform independent (currently supports GCP; AWS, Azure to come)
  * **Modular/Extensible**
    * Plug-N-Play with user-defined task modules
    * Easily re-use, re-combine across workflows
      * Eliminates serial copy/paste
    * Easily add or customize task modules as needed 
  * **Pre-Launch Type-Checking**
    * Strongly-typed task modules 
      * Catch pipeline errors prior to runtime
    * Pre-launch validation ensures pipeline success/failure
  * **Scalable**
    * Removes resource limitations imposed by cluster-based HPCCs
  * **Elastic**
    * VM usage automatically scales to match input file sizes, computational needs
  * **Scatter-Gather Parallelism**
    * In-built logic for dividing large tasks into small chunks and re-combining
  * **Economical**
    * Preemptible/Spot instances drastically cut workflow costs

## Setting up your system
  
CloudConductor is currently designed only for *Linux* systems. 
You will need to install and configure the following tools to run your pipelines on Google Cloud:  

1. [Python](https://www.python.org/) v3.6+

    You can check your Python version by running the following command in your terminal:

    ```sh
    $ python3 -V
    Python 3.6.8
    ```

    To install the correct version of Python, visit the official Python [website](https://www.python.org/downloads/).

2. Python packages: *configobj*, *jsonschema*, *requests*

    You will need [pip](https://packaging.python.org/guides/installing-using-linux-tools/) to install the above packages.
    After installing *pip*, run the following commands in your terminal: 

    ``` sh
    # Upgrade pip
    sudo pip3 install -U pip
    
    # Install Python modules
    sudo pip3 install -U configobj jsonschema requests
    ```

3. Clone the **CloudConductor** repo

    ```sh
    # clone the repo
    git clone https://github.com/labdave/CloudConductor.git
    ```

4. [Google Cloud Platform](https://cloud.google.com/) SDK

    Follow the [instructions](https://cloud.google.com/sdk/docs/downloads-interactive) on the official Google Cloud website.

## Documentation

Get started with our full [documentation](https://cloudconductor.readthedocs.io) to explore the ways CloudConductor can streamline the development and execution of complex, multi-sample workflows typical in bioinformatics.

## Project Status

CloudConductor is actively under development. To get involved or request features, please contact [Razvan Panea](https://github.com/ripanea).

## Authors & Contributors

* [Razvan Panea](https://github.com/ripanea)
* [Alex Waldrop](https://github.com/alexwaldrop)
* [Tushar Dave](https://github.com/tushardave26)
* [Clay Parker](https://github.com/parkerc71)
* [Qiu Qin](https://github.com/qiuosier)
* [Rachel Kositsky](https://github.com/rkositsky)

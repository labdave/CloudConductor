import json
from collections import OrderedDict


class ScriptTask(object):

    def __init__(self, task_id):

        self.task_id                    = task_id
        self.instance_name              = ""
        self.parents                    = []
        self.module_name                = ""
        self.submodule_name             = ""
        self.cpu_request                = None
        self.cpu_max                    = None
        self.memory_request             = None
        self.memory_max                 = None
        self.storage_request            = None
        self.pool_name                  = ""
        self.instance_type              = ""
        self.calculate_storage          = False
        self.commands                   = OrderedDict()
        self.input_files                = []
        self.input_values               = {}
        self.output_files               = []
        self.extra_volumes              = []
        self.labels                     = []
        self.annotations                = []
        self.num_retries                = 3
        self.post_processing_required   = False
        self.update_input_required      = False
        self.force_standard             = False
        self.status                     = "IDLE"
        self.start_time                 = None
        self.end_time                   = None
        self.run_time                   = 0
        self.cost                       = 0

    def to_dict(self):
        task = OrderedDict()
        task["task_id"]                     = self.task_id
        task["instance_name"]               = self.instance_name
        task["parents"]                     = self.parents
        task["cpu_request"]                 = self.cpu_request
        task["cpu_max"]                     = self.cpu_max
        task["memory_request"]              = self.memory_request
        task["memory_max"]                  = self.memory_max
        task["storage_request"]             = self.storage_request
        task["pool_name"]                   = self.pool_name
        task["instance_type"]               = self.instance_type
        task["calculate_storage"]           = self.calculate_storage
        task["commands"]                    = self.commands
        task["input_files"]                 = self.input_files
        task["input_values"]                = self.input_values
        task["output_files"]                = self.output_files
        task["extra_volumes"]               = self.extra_volumes
        task["labels"]                      = self.labels
        task["annotations"]                 = self.annotations
        task["num_retries"]                 = self.num_retries
        task["post_processing_required"]    = self.post_processing_required
        task["update_input_required"]       = self.update_input_required
        task["force_standard"]              = self.force_standard
        task["status"]                      = self.status
        task["run_time"]                    = self.run_time
        task["start_time"]                  = self.start_time
        task["end_time"]                    = self.end_time
        task["cost"]                        = self.cost
        return task

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__)

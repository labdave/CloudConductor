import json
import logging

from jsonschema import Draft4Validator, validators

from Config.Parsers import BaseParser


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property_, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property_, subschema["default"])

        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

    return validators.extend(
        validator_class, {"properties": set_defaults},
    )


DefaultValidatingDraft4Validator = extend_with_default(Draft4Validator)


class JsonParser(BaseParser):
    # Class for parsing information from a JSON config file
    def __init__(self, config_file, config_spec_file):
        super(JsonParser, self).__init__(config_file, config_spec_file)

    def read_config(self):
        # Parse config data from JSON file and return JSON
        return self.parse_json(self.config_file)

    def validate_config(self):
        # Parse config spec file
        spec = self.parse_json(self.config_spec_file)

        # create validator object
        validator = DefaultValidatingDraft4Validator(spec)

        # validate config against schema and throw errors if invalid
        try:
            validator.validate(self.config)
        except:
            # raise invalid schema error and return messages
            logging.error("Invalid config: %s" % self.config_file)
            errors = validator.iter_errors(self.config)
            for error in sorted(errors):
                logging.error(error.message)
            raise

    def extend_with_default(self, validator_class):
        validate_properties = validator_class.VALIDATORS["properties"]

        def set_defaults(self, validator, properties, instance, schema):
            for property, subschema in properties.iteritems():
                if "default" in subschema:
                    instance.setdefault(property, subschema["default"])

            for error in validate_properties(validator, properties, instance, schema):
                yield error

            return validators.extend(
                validator_class, {"properties" : set_defaults},
            )

    @staticmethod
    def parse_json(json_file):
        # read JSON formatted instance template config
        with open(json_file, "r") as fh:
            json_data = fh.read()

        # try to load json from file, return exception otherwise
        try:
            return json.loads(json_data)

        except:
            logging.error("Config parsing error! Input File is not valid JSON: %s" % json_file)
            raise

"""Contains utility functions for calling the Official Kubernetes API
"""
import logging
import json
import copy
import collections
import time
import random
from kubernetes.client.rest import ApiException


def api_request(api_func, *args, **kwargs):
    """Sends an API request by calling api_func(*args, **kwargs) and catches the ApiException, if any.

    Args:
        api_func: A function/method/callable object that uses functions from the Kubernetes API package
        *args: Arguments for calling api_func.
        **kwargs: Keyword arguments for calling api_func.

    Returns: The response of calling api_func, most likely a dictionary.
        If an error occurs, the response will be a dictionary containing the following keys:
            status, the status returned in the ApiException
            error, the reason of the error
            headers, the header of the ApiException

    """
    for i in range(8):
        try:
            response = api_func(*args, **kwargs)
            # Convert the response to dictionary if possible
            if hasattr(response, "to_dict"):
                response = response.to_dict()
            break
        except ApiException as e:
            time.sleep(get_api_sleep(i+1))
            logging.warning("Exception when calling %s: %s" % (api_func.__name__, e))
            response = {
                "status": e.status,
                "error": e.reason,
                "headers": stringify(e.headers),
            }
            try:
                error_body = json.loads(e.body)
                response.update(error_body)
            except Exception:
                logging.debug("Issue with parsing the error body into json.")
            continue
        except ConnectionResetError as e:
            time.sleep(get_api_sleep(i+1))
            logging.warning("Exception when calling %s: %s" % (api_func.__name__, e))
            logging.debug("Will retry the request.")
            response = {
                "status": "Failed",
                "error": e,
            }
            continue
        except Exception as e:
            logging.warning("Exception when calling %s: %s" % (api_func.__name__, e))
            exception_string = str(e)
            if "Max retries" in exception_string or "NewConnectionError" in exception_string:
                time.sleep(get_api_sleep(i+1))
                logging.debug("Will retry the request.")
                response = {
                    "status": "Failed",
                    "error": e,
                }
                continue
            else:
                response = {
                    "status": "Failed",
                    "error": e,
                }
                break
    return response


def get_api_sleep(attempt):
    temp = min(200, 4 * 2 ** attempt)
    return temp / 2 + random.randrange(0, temp/2)


# This function is from the Aries package.
def stringify(obj):
    """Convert object to string.
    If the object is a dictionary-like object or list,
    the objects in the dictionary or list will be converted to strings, recursively.

    Returns: If the input is dictionary or list, the return value will also be a list or dictionary.

    """
    if isinstance(obj, collections.Mapping):
        obj = copy.deepcopy(obj)
        obj_dict = {}
        for key, value in obj.items():
            obj_dict[key] = stringify(value)
        return obj_dict
    elif isinstance(obj, list):
        str_list = []
        for item in obj:
            str_list.append(stringify(item))
        return str_list
    else:
        try:
            json.dumps(obj)
            return obj
        except TypeError:
            return str(obj)


def get_dict_value(dictionary, *keys, default=None):
    d = dictionary
    for key in keys:
        if key not in d:
            return default
        d = d.get(key, dict())
    return d

# Define the main CloudConductor directory
from os.path import dirname, abspath
CC_MAIN_DIR = dirname(dirname(abspath(__file__)))

from .GAPipeline import GAPipeline, GAPReport

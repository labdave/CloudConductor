import logging
import json

from Modules import Module

def parse_qc_report(out):
    # Return QCReport parsed from a string

    # Try loading json
    try:
        qc_string = json.loads(out)
    except:
        logging.error("Unable to load QCReport! Output is not valid JSON:\n%s" % out)
        raise

    # Try loading QCReport from json
    try:
        qc_report = _QCReport(report=qc_string)
    except:
        logging.error("JSON is valid but unable to parse into QCReport!")
        raise
    return qc_report


class _QCReportError(Exception):
    pass


class _QCReport:
    # Class ported from QCParser to hold QC data associated with samples, modules
    def __init__(self, report=None):
        # Initialize report from existing dictionary or from empty dict
        self.report     = {} if report is None else report
        self.validate()

    def fetch_values(self, sample_name, module, data_type, note_type=None):
        # Return values of search criteria that match
        if sample_name not in self.get_sample_names():
            raise _QCReportError("Sample '%s' not found in QCReport!" % sample_name)

        elif module not in self.get_modules():
            raise _QCReportError("Module '%s' not found in QCReport!" % module)

        elif data_type not in self.get_colnames():
            raise _QCReportError("Data type'%s' not found in QCReport!" % data_type)

        # Add values of any datapoints which match filters
        values = []
        for sample_data_point in self.report[sample_name]:
            if sample_data_point["Module"] == module and sample_data_point["Name"] == data_type:
                if note_type is None:
                    values.append(sample_data_point["Value"])
                elif note_type == sample_data_point["Note"]:
                    values.append(sample_data_point["Value"])
        return values

    def get_sample_names(self):
        return list(self.report.keys())

    def get_colnames(self, sample=None):
        if sample is None:
            sample_names = self.get_sample_names()
            if len(sample_names) == 0:
                return []
            sample = sample_names[0]
        # Get data colnames associated with a sample
        return [x["Name"] for x in self.get_sample_data(sample)]

    def get_modules(self, sample=None):
        # Get list of modules used to produce QCReport data
        if sample is None:
            sample_names = self.get_sample_names()
            if len(sample_names) == 0:
                return []
            sample = sample_names[0]
        return [x["Module"] for x in self.get_sample_data(sample)]

    def get_sample_data(self, sample):
        if sample not in self.get_sample_names():
            logging.error("Sample '%s' not found in QCReport!")
            raise _QCReportError("Cannot get data of non-existant sample!")
        return self.report[sample]

    def validate(self):
        # Determine whether QCReport is valid
        for sample_name, sample_data in self.report.items():
            # Make sure every data point in every sample row contains only the required fields
            for sample_column in sample_data:
                if not "".join(sorted(sample_column.keys())) == "ModuleNameNoteSourceValue":
                    logging.error("Entry in QCReport for sample %s does not contain required columns!" % sample_name)
                    raise _QCReportError("Invalid QCReport schema.")

        # Check to make sure QCReport is square
        if not self.is_square():
            # Check if
            logging.error("QCReport is not square! Not all rows have same number of columns!")
            raise _QCReportError("Invalid QCReport! Not all rows have same number of columns!")

        # Check to make sure all rows in QCReport have columns in same order
        if not self.is_ordered():
            logging.error("QCReport is not ordered! Data columns are not same for every sample or are not in same order!")
            raise _QCReportError("Invalid QCReport! Data columns are not same for every sample or are not in same order!")

    def is_square(self):
        # Return True if all rows have same number of columns
        row_len = -1
        for sample in self.get_sample_names():
            if row_len == -1:
                row_len = len(self.get_colnames(sample))
            else:
                if len(self.get_colnames(sample)) != row_len:
                    return False
        return True

    def is_ordered(self):
        # Return True if columns in every row are in same order
        row_order = ""
        for sample in self.get_sample_names():
            if row_order == "":
                row_order = "_".join(self.get_colnames(sample))
            elif "_".join(self.get_colnames(sample)) != row_order:
                return False
        return True


class _QCReportReader(Module):
    # Private base class for methods shared by modules that parse data from QCReports and
    # Make that data available to downstream modules in the pipelines
    def __init__(self, module_id, is_docker=False):
        super(_QCReportReader, self).__init__(module_id, is_docker)

    def define_input(self):
        self.add_argument("qc_report",      is_required=True)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_command(self):
        # Spit qc report to stdout for parsing
        qc_report       = self.get_argument("qc_report")
        return "cat %s !LOG2!" % qc_report


class GetNumReadsFastQC(_QCReportReader):

    def __init__(self, module_id, is_docker=False):
        super(GetNumReadsFastQC, self).__init__(module_id, is_docker)
        self.output_keys = ["nr_reads"]
        self.does_process_output = True

    def define_input(self):
        super(GetNumReadsFastQC, self).define_input()
        self.add_argument("filter_by_note", default_value=None)

    def define_output(self):
        self.add_output("nr_reads", 0, is_path=False)

    def process_cmd_output(self, out, err):
        # Parse numreads from FastQC sections of QCReport
        qc_report = parse_qc_report(out)
        sample_name     = self.get_argument("sample_name")
        filter_by_note  = self.get_argument("filter_by_note")

        # Try to parse num_reads from QCReport
        try:
            num_reads = qc_report.fetch_values(sample_name,
                                               module="FastQC",
                                               data_type="Total_Reads",
                                               note_type=filter_by_note)
        # Raise any errors
        except BaseException as e:
            logging.error("GetNumReads unable to get number of reads from QCReport!")
            if str(e) != "":
                logging.error("Receive the following err msg:\n%s" % e)
            raise

        # Get length of errors
        if len(num_reads) == 0:
            logging.error("GetNumReads could not find any FastQC num reads which matched the search criteria!")
            raise RuntimeError("GetNumReads could not determine the number of FastQC reads from the QCReport!")

        # Sum R1, R2 for paired end reads (will also work for single end reads where only one record is present)
        num_reads = sum(num_reads)

        # Update num reads
        self.set_output("nr_reads", num_reads)
        logging.debug("This is how many reads we got: %s" % num_reads)


class GetNumReadsTrimmomatic(_QCReportReader):

    def __init__(self, module_id, is_docker=False):
        super(GetNumReadsTrimmomatic, self).__init__(module_id, is_docker)
        self.output_keys = ["nr_reads"]
        self.does_process_output = True

    def define_output(self):
        self.add_output("nr_reads", 0, is_path=False)

    def process_cmd_output(self, out, err):
        # Parse numreads from FastQC sections of QCReport
        qc_report = parse_qc_report(out)
        sample_name     = self.get_argument("sample_name")

        # Try to parse num_reads from QCReport
        try:
            num_reads = qc_report.fetch_values(sample_name,
                                               module="Trimmomatic",
                                               data_type="Trimmed_Reads")

        # Raise any errors
        except BaseException as e:
            logging.error("GetNumReads unable to get number of reads from QCReport!")
            if str(e) != "":
                logging.error("Receive the following err msg:\n%s" % e)
            raise

        # Get length of errors
        if len(num_reads) == 0:
            logging.error("GetNumReads could not find any Trimmomatic num reads which matched the search criteria!")
            raise RuntimeError("GetNumReads could not determine the number of Trimmomatic reads from the QCReport!")

        # Get total number of trimmed reads (R1+R2)
        num_reads = num_reads[0]
        logging.info("Num reads: %s" % num_reads)
        # Update num reads
        self.set_output("nr_reads", num_reads)

from Modules import Splitter

class SampleSplitter(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(SampleSplitter, self).__init__(module_id, is_docker)
        self.output_keys = ["sample_id"]

    def define_input(self):
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        # Obtaining the arguments
        samples = self.get_argument("sample_id")

        # Make one split if only one sample
        if not isinstance(samples, list):
            self.make_split(split_id=samples, visible_samples=[samples])

        # Otherwise split through everything
        else:
            for sample in samples:
                self.make_split(split_id=sample, visible_samples=[sample])

    def define_command(self):
        return None


class TumorNormalSplitter(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(TumorNormalSplitter, self).__init__(module_id, is_docker)
        self.output_keys = ["sample_id"]

    def define_input(self):
        self.add_argument("sample_id",    is_required=True)
        self.add_argument("is_tumor",       is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        # Obtaining the arguments
        samples = self.get_argument("sample_id")
        is_tumor = self.get_argument("is_tumor")

        # Make one split if only one sample
        if not isinstance(samples, list):
            split_id = "tumor" if is_tumor else "normal"
            self.make_split(split_id=split_id, visible_samples=[samples])

        # Otherwise split through everything
        else:
            tumors = []
            normals = []
            for i in range(len(samples)):
                if is_tumor[i]:
                    tumors.append(samples[i])
                else:
                    normals.append(samples[i])

            if len(tumors) > 0:
                self.make_split(split_id="tumor", visible_samples=tumors)

            if len(normals) > 0:
                self.make_split(split_id="normal", visible_samples=normals)

    def define_command(self):
        return None

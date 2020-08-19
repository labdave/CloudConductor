from Modules import Module

class Clonotype(Module):
    def __init__(self, module_id, is_docker = False):
        super(Clonotype, self).__init__(module_id, is_docker)
        self.output_keys = ['ig_clones', 't_clones', 'ig_report', 't_report']

    def define_input(self):
        self.add_argument('bam',        is_required=True)
        self.add_argument('bam_idx',    is_required=True)
        self.add_argument('ig',         default_value=False)
        self.add_argument('t',          default_value=False)
        self.add_argument('nr_cpus',    default_value=2)
        self.add_argument('mem',        default_value=12)
        self.add_argument('logging',    default_value='!LOG1!')

    def define_output(self):
        ig_clones   = self.generate_unique_file_name('IG.clones.tsv')
        t_clones    = self.generate_unique_file_name('T.clones.tsv')
        ig_report   = self.generate_unique_file_name('.IG.report')
        t_report    = self.generate_unique_file_name('.T.report')

        ig          = self.get_argument('ig')
        t           = self.get_argument('t')

        if ig:
            self.add_output('ig_clones', ig_clones)
            self.add_output('ig_report', ig_report)
        if t:
            self.add_output('t_clones', t_clones)
            self.add_output('t_report', t_report)

    def define_command(self):
        bam         = self.get_argument('bam')
        threads     = self.get_argument('nr_cpus')
        logging     = self.get_argument('logging')
        ig          = self.get_argument('ig')
        t           = self.get_argument('t')

        cmd_ig      = ''
        cmd_t       = ''

        if ig:
            ig_clones   = self.get_output('ig_clones')
            ig_report   = self.get_output('ig_report')
            cmd_ig  = 'bash BCR.sh {0} {1} {2} {3} {4}'.format(bam, threads, ig_report, ig_clones, logging)
        if t:
            t_clones    = self.get_output('t_clones')
            t_report    = self.get_output('t_report')
            cmd_t   = 'bash TCR.sh {0} {1} {2} {3} {4}'.format(bam, threads, t_report, t_clones, logging)

        if cmd_ig and cmd_ig:
            return[cmd_ig, cmd_t]
        if cmd_ig:
            return cmd_ig
        if cmd_t:
            return cmd_t

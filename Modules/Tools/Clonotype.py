from Modules import Module

class Clonotype(Module):
    def __init__(self, module_id, is_docker = False):
        super(Clonotype, self).__init__(module_id, is_docker)
        self.output_keys = ['top_ig_clones', 'all_ig_clones', 'top_t_clones', 'all_t_clones', 'ig_report', 't_report']

    def define_input(self):
        self.add_argument('bam',                is_required=True)
        self.add_argument('bam_idx',            is_required=True)
        self.add_argument('ig',                 default_value=False)
        self.add_argument('t',                  default_value=False)
        self.add_argument('nr_cpus',            default_value=4)
        self.add_argument('mem',                default_value=30)
        self.add_argument('logging',            default_value='!LOG3!')

    def define_output(self):
        top_ig_clones       = self.generate_unique_file_name('IG.top.clones.tsv')
        all_ig_clones       = self.generate_unique_file_name('IG.all.clones.tsv')
        top_t_clones        = self.generate_unique_file_name('T.top.clones.tsv')
        all_t_clones        = self.generate_unique_file_name('T.all.clones.tsv')
        ig_report           = self.generate_unique_file_name('.IG.report')
        t_report            = self.generate_unique_file_name('.T.report')

        ig                  = self.get_argument('ig')
        t                   = self.get_argument('t')

        if ig:
            self.add_output('top_ig_clones',    top_ig_clones)
            self.add_output('all_ig_clones',    all_ig_clones)
            self.add_output('ig_report',        ig_report)
        if t:
            self.add_output('top_t_clones',    top_t_clones)
            self.add_output('all_t_clones',    all_t_clones)
            self.add_output('t_report',        t_report)

    def define_command(self):
        bam                 = self.get_argument('bam')
        threads             = self.get_argument('nr_cpus')
        logging             = self.get_argument('logging')
        ig                  = self.get_argument('ig')
        t                   = self.get_argument('t')

        cmd_ig              = ''
        cmd_t               = ''

        if ig:
            top_ig_clones   = self.get_output('top_ig_clones')
            all_ig_clones   = self.get_output('all_ig_clones')
            ig_report       = self.get_output('ig_report')
            cmd_ig = 'bash BCR.sh {0} {1} {2} {3} {4} {5}'.format(bam, threads, ig_report, top_ig_clones, all_ig_clones, logging)
        if t:
            top_t_clones    = self.get_output('top_t_clones')
            all_t_clones    = self.get_output('all_t_clones')
            t_report        = self.get_output('t_report')
            cmd_t = 'bash TCR.sh {0} {1} {2} {3} {4} {5}'.format(bam, threads, t_report, top_t_clones, all_t_clones, logging)

        if cmd_ig and cmd_ig:
            return[cmd_ig, cmd_t]
        if cmd_ig:
            return cmd_ig
        if cmd_t:
            return cmd_t

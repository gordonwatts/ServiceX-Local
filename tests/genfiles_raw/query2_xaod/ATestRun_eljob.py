#
# Read the submission directory as a command line argument. You can
# extend the list of arguments with your private ones later on.
# Set up (Py)ROOT.
import ROOT  # type: ignore
import optparse
from AnaAlgorithm.DualUseConfig import createAlgorithm  # type: ignore

parser = optparse.OptionParser()

ROOT.xAOD.Init().ignore()
parser.add_option('-s', '--submission-dir', dest='submission_dir',
                  action='store', type='string', default='submitDir',
                  help='Submission directory for EventLoop')
(options, args) = parser.parse_args()


# The sample handler is going to load the files form filelist.txt,
# in this context, it is an embarrassingly easy use of that object.
sh = ROOT.SH.SampleHandler()
sh.setMetaString('nc_tree', 'CollectionTree')
ROOT.SH.readFileList(sh, "ANALYSIS", "filelist.txt")
sh.printContent()

# Create an EventLoop job.
job = ROOT.EL.Job()
job.sampleHandler(sh)



# Create the algorithm's configuration.
alg = createAlgorithm('query', 'AnalysisAlg')
# later on we'll add some configuration options for our algorithm that go here

# Add our algorithm to the job
job.algsAdd(alg)
job.outputAdd(ROOT.EL.OutputStream('ANALYSIS'))

# Run the job using the direct driver.
driver = ROOT.EL.DirectDriver()
driver.submit(job, options.submission_dir)
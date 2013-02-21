from library.mrjobwrapper import runMRJob
from settings import hdfs_local_tweets_input, hdfs_local_tweets_output
from langdetectmr import LangDetectTwitter

class LangDetectMRJobRunner(object):
	@staticmethod
	def detect(infiles, outfile):
		mr_class = LangDetectTwitter
		runMRJob(mr_class,
			outfile,
			infiles,
			mrJobClassParams = {'job_id': 'as'},
			args = [],
			jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout':86400000}
		)

	@staticmethod
	def run():
		input_files = []
		input_files.append(hdfs_local_tweets_input)
		output_file = hdfs_local_tweets_output
		LangDetectMRJobRunner.detect(input_files, output_file)

if __name__ == '__main__':
	LangDetectMRJobRunner.run()

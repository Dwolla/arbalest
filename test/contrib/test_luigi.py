import unittest

from mock import Mock, create_autospec
from arbalest.redshift import S3CopyPipeline

from arbalest.contrib.luigi import PipelineTask


class PipelineTaskShould(unittest.TestCase):
    def test_run_given_pipeline(self):
        pipeline = create_autospec(S3CopyPipeline)
        pipeline.__name__ = 'S3CopyPipeline'
        pipeline.get = Mock()

        pipeline_task = PipelineTask(pipeline)
        pipeline_task.run()

        self.assertEqual(pipeline.get.called, True)

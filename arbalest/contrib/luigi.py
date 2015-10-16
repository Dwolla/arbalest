from __future__ import absolute_import
import luigi


class PipelineTask(luigi.Task):
    pipeline = luigi.Parameter()

    @property
    def task_family(self):
        return self.pipeline.__name__

    def run(self):
        self.pipeline.get().run()

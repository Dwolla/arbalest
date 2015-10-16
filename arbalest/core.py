class SchemaException(Exception):
    pass


class PipelineException(Exception):
    pass


class PipelineStep(object):
    def run(self):
        pass

    def validate(self):
        pass


class Pipeline(object):
    def __init__(self):
        self.pipeline_steps = []

    def steps(self):
        return self.pipeline_steps

    def run(self):
        self.__validate_has_steps()
        for step in self.steps():
            step.run()
        return self

    def validate(self):
        self.__validate_has_steps()
        for step in self.steps():
            step.validate()
        return self

    def __validate_has_steps(self):
        if not self.steps():
            raise PipelineException('Cannot run pipeline with no steps')

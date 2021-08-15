import pandas as pd
from tasrif.processing_pipeline import  ProcessingOperator, SplitJoinOperator, NoopOperator, ProcessingPipeline


class ListofRangeOfNumbersOperator(ProcessingOperator):

    def __init__(self, list_count, numbers_count):

        self.list_count = list_count
        self.numbers_count = numbers_count

    def process(self, *args):
        output = []
        for i in range(0, self.list_count):
            output.append(list(range(i*self.numbers_count, (i + 1)*self.numbers_count)))

        return output


class MutiplybyOperator(ProcessingOperator):

    def __init__(self, factor):
        self.factor = factor

    def process(self, *args):
        output = []
        for arg in args:
            output_item = []
            for i in arg:
                output_item.append(i*self.factor)
            output.append(output_item)

        return output


class AddOperator(ProcessingOperator):

    def __init__(self, data_count):
        self.data_count = data_count


    def process(self, *args):
        input_data = []
        for arg in args[0]:
            input_data.append(arg[0])
        input_data = (list(zip(*input_data)))
        output = []
        for arg in input_data:
            total = 0
            for i in arg:
                total += i
            output.append(total)

        return output

pipeline = ProcessingPipeline([
    ListofRangeOfNumbersOperator(3, 10),
    SplitJoinOperator([
            NoopOperator(),
            MutiplybyOperator(2)],
        AddOperator(3))])

pipeline.process()



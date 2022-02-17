"""Python package that defines the processing framework of tasrif
"""

from tasrif.processing_pipeline.compose_operator import ComposeOperator
from tasrif.processing_pipeline.map_iterable_operator import MapIterableOperator
from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
from tasrif.processing_pipeline.noop_operator import NoopOperator
from tasrif.processing_pipeline.observer import Observer
from tasrif.processing_pipeline.pandas_operator import PandasOperator
from tasrif.processing_pipeline.parallel_operator import ParallelOperator
from tasrif.processing_pipeline.print_operator import PrintOperator
from tasrif.processing_pipeline.processing_operator import ProcessingOperator
from tasrif.processing_pipeline.reduce_processing_operator import (
    ReduceProcessingOperator,
)
from tasrif.processing_pipeline.scoped_processing_operator import (
    ScopedProcessingOperator,
)
from tasrif.processing_pipeline.sequence_operator import SequenceOperator
from tasrif.processing_pipeline.split_operator import SplitOperator
from tasrif.processing_pipeline.variable import Variable

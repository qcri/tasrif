"""Python package that defines the processing framework of tasrif
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator
from tasrif.processing_pipeline.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.sequence_operator import SequenceOperator
from tasrif.processing_pipeline.compose_operator import ComposeOperator
from tasrif.processing_pipeline.noop_operator import NoopOperator
from tasrif.processing_pipeline.generator_processing_operator import GeneratorProcessingOperator
from tasrif.processing_pipeline.split_join_operator import SplitJoinOperator
from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
from tasrif.processing_pipeline.reduce_processing_operator import ReduceProcessingOperator

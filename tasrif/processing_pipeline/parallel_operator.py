"""Module that defines the RayOperator class
"""
import ray
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ParallelOperator(ProcessingOperator):
    """Interface specification of an operator that uses ray
    """

    def __init__(self, num_processes=1):
        """Constructs a ray operator. Replaces `_process` with `_process_ray`

        Args:
            num_processes: int
                number of logical processes to use to process the operator

        """
        super().__init__()
        self.num_processes = num_processes

        if ray.is_initialized() and (self.num_processes == 1):
            ray.shutdown()

        if self.num_processes > 1:
            self.init_ray(self.num_processes)
            self._process = self._process_ray

    def _process_ray(self, *args):
        """
        Ray version of _process

        Args:
            *args (list of ProcessingOperator):
                Variable number of ProcessingOperator to be applied on a dataframe

        """

    @staticmethod
    @ray.remote
    def _process_operator(operator, *operator_args):
        return operator.process(*operator_args)

    @staticmethod
    def init_ray(num_cpus):
        """
        Initialized Ray cluster.
        If the cluster is already initialized, and the number of logical processes being used
        is different than `num_cpus`, restart the cluster with `num_cpus`

        Args:
            num_cpus: int
                number of logical processes to use to process the operator

        """
        if ray.is_initialized():
            cluster_resources = ray.cluster_resources()
            if cluster_resources['CPU'] != num_cpus:
                ray.shutdown()

        ray.init(num_cpus=num_cpus, ignore_reinit_error=True)

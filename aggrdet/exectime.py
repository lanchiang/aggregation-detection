# Created by lan at 2021/3/1
import luigi


# class TimeTaskMixin:
#     """
#     A mixin that when added to a luigi task, will print out
#     the tasks execution time to standard out, when the task is
#     finished
#     """
#     @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
#     def print_execution_time(self, processing_time):
#         print("### Processing time ###: " + str(processing_time))
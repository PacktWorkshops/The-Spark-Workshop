# from utilities01_py.helper_python import *
# from pyspark.sql import SparkSession
# from pyspark import RDD
# import time
# from py4j.java_gateway import (
#     JavaGateway, CallbackServerParameters, GatewayParameters,
#     launch_gateway)
#
#
# class SparkListener(object):
#     def onApplicationEnd(self, applicationEnd):
#         pass
#     def onApplicationStart(self, applicationStart):
#         pass
#     def onBlockManagerRemoved(self, blockManagerRemoved):
#         pass
#     def onBlockUpdated(self, blockUpdated):
#         pass
#     def onEnvironmentUpdate(self, environmentUpdate):
#         pass
#     def onExecutorAdded(self, executorAdded):
#         pass
#     def onExecutorMetricsUpdate(self, executorMetricsUpdate):
#         pass
#     def onExecutorRemoved(self, executorRemoved):
#         pass
#     def onJobEnd(self, jobEnd):
#         pass
#     def onJobStart(self, jobStart):
#         pass
#     def onOtherEvent(self, event):
#         pass
#     def onStageCompleted(self, stageCompleted):
#         pass
#     def onStageSubmitted(self, stageSubmitted):
#         pass
#     def onTaskEnd(self, taskEnd):
#         pass
#     def onTaskGettingResult(self, taskGettingResult):
#         pass
#     def onTaskStart(self, taskStart):
#         pass
#     def onUnpersistRDD(self, unpersistRDD):
#         pass
#     class Java:
#         implements = ["org.apache.spark.scheduler.SparkListenerInterface"]
#
# class TaskEndListener(SparkListener):
#     def onTaskEnd(self, taskEnd):
#         print(taskEnd.toString())
#
#
#
# if __name__ == "__main__":
#     # main method of Exercise4_01.py comes here
#     session: SparkSession = create_session(2, "Failure Exploration")
#     time.sleep(10)
#     listener = TaskEndListener()
#
#     port = launch_gateway()
#
#     # connect python side to Java side with Java dynamic port and start python
#     # callback server with a dynamic port
#     gateway = JavaGateway(
#     gateway_parameters=GatewayParameters(port=port),
#     callback_server_parameters=CallbackServerParameters(port=0))
#
#
#     # session.sparkContext._gateway.start_callback_server()
#     session.sparkContext._jsc.sc().addSparkListener(listener)
#     session.sparkContext.parallelize(range(100), 3).count()
#

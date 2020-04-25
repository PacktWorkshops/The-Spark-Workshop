class RetryListener(object):
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]

    def onTaskStart(self, task_start):
        task_id = task_start.taskInfo().taskId()
        task_attempt = task_start.taskInfo().attemptNumber()
        stage_id = task_start.stageId()
        stage_attempt_id = task_start.stageAttemptId()
        print('@@ Starting task {} | attempt {} in stage {} | attempt {}'.format(task_id, task_attempt, stage_id,
                                                                                stage_attempt_id))

    def onTaskEnd(self, task_end):
        task_id = task_end.taskInfo().taskId()
        task_attempt = task_end.taskInfo().attemptNumber()
        stage_id = task_end.stageId()
        stage_attempt_id = task_end.stageAttemptId()
        success = task_end.taskInfo().failed()
        print('@@ Ending task {} | attempt {} in stage {} | attempt {} | has failed? {}'.format(task_id, task_attempt, stage_id,
                                                                                       stage_attempt_id, success))

    def onStageCompleted(self, _):
        pass

    def onStageSubmitted(self, _):
        pass

    def onTaskGettingResult(self, _):
        pass

    def onJobStart(self, _):
        pass

    def onJobEnd(self, _):
        pass

    def onEnvironmentUpdate(self, _):
        pass

    def onBlockManagerAdded(self, _):
        pass

    def onBlockManagerRemoved(self, _):
        pass

    def onUnpersistRDD(self, _):
        pass

    def onApplicationStart(self, _):
        pass

    def onApplicationEnd(self, _):
        pass

    def onExecutorMetricsUpdate(self, _):
        pass

    def onExecutorAdded(self, _):
        pass

    def onExecutorRemoved(self, _):
        pass

    def onExecutorBlacklisted(self, _):
        pass

    def onExecutorBlacklistedForStage(self, _):
        pass

    def onNodeBlacklistedForStage(self, _):
        pass

    def onExecutorUnblacklisted(self, _):
        pass

    def onNodeBlacklisted(self, _):
        pass

    def onNodeUnblacklisted(self, _):
        pass

    def onBlockUpdated(self, _):
        pass

    def onSpeculativeTaskSubmitted(self, _):
        pass

    def onOtherEvent(self, _):
        pass

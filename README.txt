Q1: What is the default block size on HDFS? What is the default replication factor of HDFS on Dataproc?
128MB and 2 replication factor

Q2: What is the completion time of the task?
the completion time is 3min and 31 seconds

Q3: Is the performance getting better or worse in terms of completion time? Briefly explain.
After change parameters the completion time is 2 minute and 23 seconds which is faster/better, we add one work nodes to make it faster. 

Q4: Is the performance getting better or worse in terms of completion time? Briefly explain.
After we change the block size the completion time is 2 minutes and 10 seconds which is faster that's because smaller block sizes yield higher IOPS and the latency is smaller.

Q5: Does the job still finish? Do you observe any difference in the completion time? Briefly explain your observations.
Before kill one worker the completion time is 28min 43 seconds and after we kill it the completion time is 59mins 32 seconds. The job still finish and the difference was becasue the #worker reduceds

Q6: Do you observe any difference in the completion time? Briefly explain.
It finished in 28minutes and 23 seconds and there is not much difference.

Q7: is the performance getting better or worse in terms of completion time? Briefly explain.
The completion time is 29 mins and 43 seconds which is slightly worse that's because the file size increase and larger block size may have slightly better performance

Q8: What is the completion time of the task?
The completion time is 1 hour and 2 minutes
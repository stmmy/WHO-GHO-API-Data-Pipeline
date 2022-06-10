from asyncio import QueueEmpty
import sys
import os
from turtle import update
import validate
import process
import transform
import pipeline_queue as pq

#Initialize and create basis for queue
queue = pq.pipeline_queue()
df = queue.GetStageFiles("staging")
queue.AddToQueue(df)

while queue.CheckIfDone() != True:
    next_file = queue.GetNextFile()
    queue.UpdateQueue(next_file, "VALIDATING")
    validate.validate(next_file)
    queue.UpdateQueue(next_file, "TRANSFORMING")
    transform.transform(next_file)
    queue.UpdateQueue(next_file, "PROCESSING")
    updated_file_name = next_file[0:next_file.find(".")] + ".csv"
    process.process(updated_file_name)
    if updated_file_name in os.listdir("C:/API&Pipeline/archived"):
        queue.UpdateQueue(next_file, "DONE")
    else:
        queue.UpdateQueue(next_file, "FAILED")








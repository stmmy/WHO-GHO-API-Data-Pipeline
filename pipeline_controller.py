import os
import warnings
import validate
import process
import transform
import pipeline_queue as pq


#Initialize and create basis for queue
warnings.filterwarnings("ignore") #Constant warnings are thrown b/c of deprecated pandas functions
queue = pq.pipeline_queue()

#Main Queue controller loop
if len(os.listdir("C:/API&Pipeline/staging")) > 0:
    df = queue.GetStageFiles("staging")
    queue.AddToQueue(df)
    
    while queue.CheckIfDone() != True:
        next_file = queue.GetNextFile()

        #Validation
        queue.UpdateQueue(next_file, "VALIDATING")
        validate_file = validate.validate(next_file)
        if validate_file == False:
            print("{} Failed Validation".format(next_file))
            queue.UpdateQueue(next_file, "FAILED")
            queue.UpdateLogs()
            continue

        #Transformation
        queue.UpdateQueue(next_file, "TRANSFORMING")
        transform_file = transform.transform(next_file)
        if transform_file == False:
            print("{} Failed Transformation".format(next_file))
            queue.UpdateQueue(next_file, "FAILED")
            queue.UpdateLogs()
            continue

        #Processing
        queue.UpdateQueue(next_file, "PROCESSING")
        updated_file_name = next_file[0:next_file.find(".")] + ".csv"
        process_file = process.process(updated_file_name)
        if process_file == True:
            queue.UpdateQueue(next_file, "DONE")
        else:
            queue.UpdateQueue(next_file, "FAILED")
        queue.UpdateLogs()
        queue.ClearLocalLogs()

    print("Queue Finished")
else:
    print("No Files in staging")

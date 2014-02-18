from remoteq.queues import  LSFBSub
from remoteq.core.batch import DescriptorQ, load_queue

q = load_queue(LSFBSub, "my_server_configuration,your_name")  
desc1 = DescriptorQ(q, command="./sleepy 1", input_directory=".", output_directory=".", overwrite_submission_id="simu1")
desc2 = DescriptorQ(q, command="./sleepy 2", input_directory=".", output_directory=".", overwrite_submission_id="simu2")

print "Handling job 1"
desc1.job()
print "Handling job 2"
desc2.job()

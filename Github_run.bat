C:\spark\bin.\spark-submit --class "Github"  --master spark://192.168.99.1:7077 --deploy-mode cluster --executor-memory 4G --total-executor-cores 4 --executor-cores 4 --driver-cores 2 --driver-memory 2G --num-executors 1 C:\Github.jar %1 %2
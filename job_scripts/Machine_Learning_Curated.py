import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1734549730643 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1734549730643")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1734549648654 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1734549648654")

# Script generated for node Join
SqlQuery812 = '''
select * 
from accelerometer_trusted a
join step_trainer_trusted t on a.timestamp = t.sensorreadingtime;
'''
Join_node1734550127402 = sparkSqlQuery(glueContext, query = SqlQuery812, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1734549730643, "step_trainer_trusted":StepTrainerTrusted_node1734549648654}, transformation_ctx = "Join_node1734550127402")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1734550251822 = glueContext.getSink(path="s3://stedi-lakehouse/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1734550251822")
MachineLearningCurated_node1734550251822.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1734550251822.setFormat("glueparquet", compression="snappy")
MachineLearningCurated_node1734550251822.writeFrame(Join_node1734550127402)
job.commit()
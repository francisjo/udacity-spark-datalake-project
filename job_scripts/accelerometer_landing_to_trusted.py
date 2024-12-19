import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1734533334452 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1734533334452")

# Script generated for node Customer Trusted
CustomerTrusted_node1734533081497 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734533081497")

# Script generated for node Join
Join_node1734533475592 = Join.apply(frame1=AccelerometerLanding_node1734533334452, frame2=CustomerTrusted_node1734533081497, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1734533475592")

# Script generated for node Change Schema
ChangeSchema_node1734543493792 = ApplyMapping.apply(frame=Join_node1734533475592, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1734543493792")

# Script generated for node Drop Duplicates
DropDuplicates_node1734536483684 =  DynamicFrame.fromDF(ChangeSchema_node1734543493792.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734536483684")

# Script generated for node Amazon S3
AmazonS3_node1734536567515 = glueContext.getSink(path="s3://stedi-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734536567515")
AmazonS3_node1734536567515.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1734536567515.setFormat("glueparquet", compression="gzip")
AmazonS3_node1734536567515.writeFrame(DropDuplicates_node1734536483684)
job.commit()
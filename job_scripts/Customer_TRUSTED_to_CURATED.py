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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1734544394174 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1734544394174")

# Script generated for node Customer Trusted
CustomerTrusted_node1734544357759 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734544357759")

# Script generated for node Join
Join_node1734544497078 = Join.apply(frame1=CustomerTrusted_node1734544357759, frame2=AccelerometerTrusted_node1734544394174, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1734544497078")

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node1734544609515 = ApplyMapping.apply(frame=Join_node1734544497078, mappings=[("serialNumber", "string", "serialNumber", "string"), ("birthDay", "string", "birthDay", "string"), ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"), ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "registrationDate", "long"), ("customerName", "string", "customerName", "string"), ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"), ("email", "string", "email", "string"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="DropAccelerometerFields_node1734544609515")

# Script generated for node Drop Duplicates
DropDuplicates_node1734544900582 =  DynamicFrame.fromDF(DropAccelerometerFields_node1734544609515.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734544900582")

# Script generated for node Customer Curated
CustomerCurated_node1734544937838 = glueContext.getSink(path="s3://stedi-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1734544937838")
CustomerCurated_node1734544937838.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1734544937838.setFormat("glueparquet", compression="gzip")
CustomerCurated_node1734544937838.writeFrame(DropDuplicates_node1734544900582)
job.commit()
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer Curated
CustomerCurated_node1734547840080 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1734547840080")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1734547825974 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1734547825974")

# Script generated for node Renamed keys for Customer curated
RenamedkeysforCustomercurated_node1734547924304 = ApplyMapping.apply(frame=CustomerCurated_node1734547840080, mappings=[("serialnumber", "string", "serialnumber1", "string")], transformation_ctx="RenamedkeysforCustomercurated_node1734547924304")

# Script generated for node join
StepTrainerLanding_node1734547825974DF = StepTrainerLanding_node1734547825974.toDF()
RenamedkeysforCustomercurated_node1734547924304DF = RenamedkeysforCustomercurated_node1734547924304.toDF()
join_node1734547863599 = DynamicFrame.fromDF(StepTrainerLanding_node1734547825974DF.join(RenamedkeysforCustomercurated_node1734547924304DF, (StepTrainerLanding_node1734547825974DF['serialnumber'] == RenamedkeysforCustomercurated_node1734547924304DF['serialnumber1']), "left"), glueContext, "join_node1734547863599")

# Script generated for node Drop Null Serial number1
SqlQuery809 = '''
select * from myDataSource
where serialnumber1 != ''
'''
DropNullSerialnumber1_node1734549243554 = sparkSqlQuery(glueContext, query = SqlQuery809, mapping = {"myDataSource":join_node1734547863599}, transformation_ctx = "DropNullSerialnumber1_node1734549243554")

# Script generated for node Drop Customer Fields
DropCustomerFields_node1734548080005 = ApplyMapping.apply(frame=DropNullSerialnumber1_node1734549243554, mappings=[("sensorReadingTime", "long", "sensorReadingTime", "long"), ("serialNumber", "string", "serialNumber", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="DropCustomerFields_node1734548080005")

# Script generated for node Drop Duplicates
DropDuplicates_node1734548172402 =  DynamicFrame.fromDF(DropCustomerFields_node1734548080005.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734548172402")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1734548241551 = glueContext.getSink(path="s3://stedi-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1734548241551")
StepTrainerTrusted_node1734548241551.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1734548241551.setFormat("glueparquet", compression="snappy")
StepTrainerTrusted_node1734548241551.writeFrame(DropDuplicates_node1734548172402)
job.commit()
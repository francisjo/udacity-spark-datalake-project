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

# Script generated for node Customer Landing
CustomerLanding_node1734526835622 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1734526835622")

# Script generated for node Share with Research
SqlQuery590 = '''
select * from myDataSource
WHERE sharewithresearchasofdate != 0
'''
SharewithResearch_node1734526873384 = sparkSqlQuery(glueContext, query = SqlQuery590, mapping = {"myDataSource":CustomerLanding_node1734526835622}, transformation_ctx = "SharewithResearch_node1734526873384")

# Script generated for node Customer Trusted
CustomerTrusted_node1734518774091 = glueContext.getSink(path="s3://stedi-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1734518774091")
CustomerTrusted_node1734518774091.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1734518774091.setFormat("glueparquet", compression="gzip")
CustomerTrusted_node1734518774091.writeFrame(SharewithResearch_node1734526873384)
job.commit()
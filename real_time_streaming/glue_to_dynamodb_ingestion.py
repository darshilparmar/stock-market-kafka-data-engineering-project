import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

empDf = glueContext.create_dynamic_frame.from_catalog(
        database="employee_data_db",
        table_name="json_inbox",
        transformation_ctx = "s3_employee_new_json"
        )
        
incrementalEmpDf = empDf.toDF()
print(incrementalEmpDf.count())

if incrementalEmpDf.count() == 0:
    print("No New records were received, Do not ingest anything into DynamoDb")
else:
    print("Incremental Data Received..")
    print(incrementalEmpDf)
    
    dynamoDf = glueContext.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={"dynamodb.input.tableName": "employee_data",
            "dynamodb.throughput.read.percent": "1.0",
            "dynamodb.splits": "100"
        }
    )
    
    existingDyanmoDf = dynamoDf.toDF()
    
    resultDf = None
    
    if existingDyanmoDf.count() != 0:
        print("Dynamo DB Table is not empty, so join will happen")
        existingDyanmoDfRenamed = existingDyanmoDf.select("emp_name").withColumnRenamed("emp_name","existing_employee_name")
        
        print("Perform Join condition")
        joinedDf = incrementalEmpDf.join(existingDyanmoDfRenamed, incrementalEmpDf.emp_name == existingDyanmoDfRenamed.emp_name, "left")
        
        print("Number of records after join = ",joinedDf.count())
        resultDf = joinedDf.filter("existing_employee_name is null")
        resultDf.drop("existing_employee_name")
    else:
        print("Dynamo DB Table is empty, so no join will happen")
        resultDf = incrementalEmpDf
        
    resultDynamicDf = DynamicFrame.fromDF(resultDf, glueContext, "resultDf")
    
    try:
        glueContext.write_dynamic_frame_from_options(
            frame=resultDynamicDf,
            connection_type="dynamodb",
            connection_options={"dynamodb.output.tableName": "employee_data",
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
        print("Data write in DyanamoDB was successful")
    except Exception as err:
        print("Data write in DyanamoDB was not successful : ",str(err))

job.commit()
from databricksx12.edi import EDI
from databricksx12.hls import HealthcareManager
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Union, Optional
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, MapType, ArrayType
from pyspark.sql import Row
from pyspark.sql.functions import col


class ProcessingResult:
    """Container for processing results with error handling"""   
    def __init__(self, row_id, success, data = None, 
                 error_message = None, error_func = None, 
                 processing_timestamp = None):
        self.row_id = row_id
        self.success = success
        self.data = data
        self.error_message = error_message
        self.error_func = error_func
        self.processing_timestamp = processing_timestamp or datetime.now().isoformat()
    
    def to_dict(self):
        """Convert to dictionary for DataFrame serialization"""
        return {
            "ih_row_id": self.row_id,
            "ih_success": self.success,
            "ih_error_message": self.error_message,
            "ih_error_type": self.error_stage,
            "ih_processing_timestamp": self.processing_timestamp
        }
    
    def to_row(self) -> Row:
        """Convert to PySpark Row"""
        return Row(**self.to_dict())


# @param data is row data from the dataframe
# @param func is the function to process the data
# @param xargs are additional arguments to pass to the function
# @return a ProcessingResult object
def process_edi(processing_result, func, **xargs):
    if processing_result.data is None: #if the data is None, that means we encountered a previous error and to return immediately
        return processing_result 
    else:
        try:
            return ProcessingResult(
                row_id = processing_result.row_id,
                success = True,
                data = func(processing_result.data, **xargs),
                error_message = None,
                error_func = None,
                processing_timestamp = ProcessingResult.processing_timestamp + [(str(func) + ":" + datetime.now().isoformat())]
            )
        
        except Exception as e:
            error_message = str(e)
            stack_trace = traceback.format_exc()
            
             return ProcessingResult(
                processing_result.row_id,
                False,
                None,
                f"{error_message}\nStack trace: {stack_trace}",
                str(func),
                datetime.now().isoformat()
            )

CORES = 96 * 2 # 96 cores per node, 2 nodes
ediType = "835"
ediYear = "2022"
schema = "prod_bronze.dts_ops"
sourceTable = f"{schema}.x12_{ediType}_stg"
edi_manager = HealthcareManager()

df_pending_rows = spark.sql(f"SELECT id, ediContent FROM {sourceTable} WHERE processed IS NULL") #Order Not needed since this will run in parallel-- ORDER BY id ASC")

result = (
    df_pending_rows.
        rdd.
        map(lambda row: EDI(row.ediContent)).
        flatMap(lambda edi: hm.flatten(edi)).
        coalesce(CORES). #avoid the network shuffle with coalesce vs repartition
        map(lambda x: hm.flatten_to_json(x)).
        map(lambda x: json.dumps(x))
    )

claims_df = spark.read.json((result.filter(lambda x: x.success).map(lambda x: x.data)))
#save claims_df to delta table TODO

run_result_df = result.map(lambda x: x.to_row()).toDF()
#save run_result_df to delta table TODO










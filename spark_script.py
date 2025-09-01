import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default data quality ruleset
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

predicate_pushdown = "region in ('ca','gb','us')"

# Read data with pushdown predicate for filtering specific regions
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="de-yt-raw",
    table_name="raw_statistics",
    transformation_ctx="datasource",
    push_down_predicate=predicate_pushdown
)

# Apply mapping with consistent data types (match source schema)
applymapping = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="applymapping"
)

# Resolve choice to handle ambiguous columns
resolvechoice = ResolveChoice.apply(
    frame=applymapping,
    choice="make_struct",
    transformation_ctx="resolvechoice"
)

# Drop null fields
dropnullfields = DropNullFields.apply(
    frame=resolvechoice,
    transformation_ctx="dropnullfields"
)

# Evaluate data quality (optional)
EvaluateDataQuality().process_rows(
    frame=dropnullfields,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Convert to DataFrame, coalesce to 1 partition for output
datasink_df = dropnullfields.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink_df, glueContext, "df_final_output")

# Write to S3 as Parquet with partition by region
datasink = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://de-on-yt-cleansed-dev-2003/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format_options={"compression": "snappy"},
    transformation_ctx="datasink"
)

job.commit()

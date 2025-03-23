import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Tracks
Tracks_node1742664967462 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-date-with-data-project/staging/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="Tracks_node1742664967462")

# Script generated for node Artist
Artist_node1742664965273 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-date-with-data-project/staging/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="Artist_node1742664965273")

# Script generated for node Album
Album_node1742664966363 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-date-with-data-project/staging/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="Album_node1742664966363")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1742665070788 = Join.apply(frame1=Artist_node1742664965273, frame2=Album_node1742664966363, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinAlbumArtist_node1742665070788")

# Script generated for node Join with Tracks
JoinwithTracks_node1742665231338 = Join.apply(frame1=Tracks_node1742664967462, frame2=JoinAlbumArtist_node1742665070788, keys1=["id"], keys2=["id"], transformation_ctx="JoinwithTracks_node1742665231338")

# Script generated for node Drop Fields
DropFields_node1742665490355 = DropFields.apply(frame=JoinwithTracks_node1742665231338, paths=[], transformation_ctx="DropFields_node1742665490355")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1742665490355, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742664957724", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1742669062636 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1742665490355, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-date-with-data-project/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1742669062636")

job.commit()
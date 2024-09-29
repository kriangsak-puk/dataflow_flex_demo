
import argparse
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

"""Parses the parameters provided on the command line and runs the pipeline."""
argv=None
parser = argparse.ArgumentParser()
parser.add_argument("--input_gcs_path", required=True, help="Input GCS path for the CSV file")
parser.add_argument("--output_bq_dataset", required=True, help="Output BigQuery dataset")
parser.add_argument("--output_bq_table", required=True, help="Output BigQuery table")
parser.add_argument("--project", required=True, help="Google Cloud project ID")
parser.add_argument("--location", required=False, default="us-central1", help="GCP location")
parser.add_argument("--job_name", required=True, help="Job name special charecter accept only '-' hyphen")
parser.add_argument("--temp_location", default="gs://personal-poc-de-project-peter/temp", help="Temporary location for pipeline artifacts")
parser.add_argument("--runner", required=False, default="DirectRunner", help="Beam runner default DirectRunner")

args, pipeline_args = parser.parse_known_args(argv)

print(pipeline_args)
# pipeline option when you need to run it at Cloud Dataflow
if args.runner == "DataflowRunner":
    beam_options = PipelineOptions(
        pipeline_args,
        runner="DataflowRunner",
        project=args.project,
        job_name=args.job_name,
        temp_location=args.location,
        region=args.location)
else:
    beam_options = None

# table referece output willbe Project:datasetid.tableid
table_spec = bigquery.TableReference(
    projectId=args.project,
    datasetId=args.output_bq_dataset,
    tableId=args.output_bq_table)

# column_name:BIGQUERY_TYPE, ...
table_schema = "id:STRING, first_name:STRING, last_name:STRING, email:STRING, gender:STRING, age:integer"

# file location
source_file = args.input_gcs_path

# temp location
temp_location = args.temp_location


class gender_abbv2(beam.DoFn):
    """DoFn class to convert gender full names to abbreviations."""
    def process(self, object):
        gender_full_name = {"FEMALE": "F", "MALE" : "M"}
        if object.gender.upper() in gender_full_name: # use upper() to handle case sensitive
            new_object = dict(object._asdict()) # since beam.DoFn took input here as a tuple which is mutable we need to create new object which contain transform
            new_object["gender"] = gender_full_name[object.gender.upper()]
            yield new_object
        else:
            new_object = dict(object._asdict())
            new_object["gender"] = "NA"
            yield new_object

with beam.Pipeline(options=beam_options) as pipeline:
    result = pipeline | "Read from text" >> beam.io.ReadFromCsv(source_file)   \
                      | "Filter out age > 20" >> beam.Filter(lambda object: object.age > 20) \
                      | "Change gender abbv" >> beam.ParDo(gender_abbv2()) 
                    #   | "Print output" >> beam.Map(print)

    # save_result = result | "save to Bigquery" >> beam.io.WriteToBigQuery(
    #                                                 table=table_spec,
    #                                                 schema=table_schema,
    #                                                 custom_gcs_temp_location=temp_location,
    #                                                 method="FILE_LOADS",
    #                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)




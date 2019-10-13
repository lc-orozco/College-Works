import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection
class DateTimeFn(beam.DoFn):
  def process(self, element):
    record = element
    case = record.get('Case_Number')
    crime = record.get('Primary_Type')
    desc = record.get('Description')
    datetime = record.get('Date')
    block = record.get('Block')
    arrest = record.get('Arrest')
    district = record.get('District')
    ward = record.get('Ward')
    comm = record.get('Community_Area')  

    # Parses date from String
    n_date = datetime[6:10] + "-" + datetime[0:2] + "-" + datetime[3:5]

    # Returns date and time in a single stamp
    return [(case, crime, desc, n_date, block, arrest, district, ward, comm)]

class MakeRecordFn(beam.DoFn):
  def process(self, element):
     case, crime, desc, n_date, block, arrest, district, ward, comm = element
     record = { 'Case_Number': case, 'Crime': crime, 'Description': desc, 'Date': n_date, 'Street_Address': block, 'Arrest': arrest, 'District': district, 'Ward': ward, 'Community_Area': comm }
     return [record] 

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM chicago_crime_2001.crime_data_aggregated LIMIT 100000'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection
    datetime_pcoll = query_results | 'Extract DateTime' >> beam.ParDo(DateTimeFn())

    # write PCollection to log file
    datetime_pcoll | 'Write to log 2' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = datetime_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':chicago_crime_2001.Crime_Data_APR_Revised'
    table_schema = 'Case_Number:STRING,Crime:STRING,Description:STRING,Date:DATE,Street_Address:STRING,Arrest:BOOL,District:INTEGER,Ward:INTEGER,Community_Area:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

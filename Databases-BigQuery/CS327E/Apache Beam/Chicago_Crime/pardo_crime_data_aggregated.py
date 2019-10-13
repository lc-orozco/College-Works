import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection
class DateTimeFn(beam.DoFn):
  def process(self, element):
    record = element
    idn = record.get('idNumber')
    case = record.get('Case_Number')
    datetime = record.get('Date')
    crime = record.get('Primary_Type')

    # Parses date from String
    n_date = datetime[6:10] + "-" + datetime[0:2] + "-" + datetime[3:5]

    # Parses time from String
    if (datetime[20:21] == 'PM'):
      hour = int(datetime[11:13]) + 12
      n_time = str(hour) + datetime[13:19]
      
    else:
      n_time = datetime[11:19]

    n_date = n_date + " " + n_time

    # Returns date and time in a single stamp
    return [(idn, case, crime, n_date)]

# DoFn performs on each element in the input PCollection
#class TimeFn(beam.DoFn):
#  def process(self, element):
#    record = element
#    time = record.get('Date')
#
#    if (time[20:21] == 'PM'):
#      hour = int(time[11:12]) + 12
#      n_time = str(hour) + time[13:18]
#    else:
#      n_time = time[11:18]
#
#    return [n_time]

# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     idn, case, crime, datetime = element
     record = { 'idNumber': idn, 'Case_Number': case, 'Primary_Type': crime, 'DateTime': datetime }
     return [record] 

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM chicago_crime_2001.crime_data_aggregated LIMIT 10000'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection
    datetime_pcoll = query_results | 'Extract DateTime' >> beam.ParDo(DateTimeFn())

    # write PCollection to log file
    datetime_pcoll | 'Write to log 2' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = datetime_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':chicago_crime_2001.Crime_Data_DateTime'
    table_schema = 'idNumber:STRING,Case_Number:STRING,Primary_Type:STRING,DateTime:STRING'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

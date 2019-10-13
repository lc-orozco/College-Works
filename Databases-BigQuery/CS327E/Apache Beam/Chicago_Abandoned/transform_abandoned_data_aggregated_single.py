import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection
class RequestNumFn(beam.DoFn):
  def process(self, element):
    record = element
    req = record.get('Service_Request_Number')
    date = record.get('Creation_Date')
    complete = record.get('Completion_Date')
    status = record.get('Status')
    typ = record.get('Type_of_Service_Request')
    lic = record.get('License_Plate')
    veh = record.get('Vehicle_Make_Model')
    col = record.get('Vehicle_Color')
    days = record.get('How_Many_Days_Has_the_Vehicle_Been_Reported_as_Parked_')
    block = record.get('Street_Address')
    ward = record.get('Ward')
    district = record.get('Police_District')
    comm = record.get('Community_Area')

    # Skips dash
    n_req1 = req[0:2]
    n_req2 = req[3:11]
    
    # Parses request number from String
    n_req = n_req1 + n_req2

    n_stat = False

    if (status.islower() == 'completed - dup') or (status.islower() == 'completed'):
        n_stat = True
    elif (status.islower() == 'open') or (status == ''):
        n_stat = False

    # Returns date and time in a single stamp
    return [(n_req, date, complete, n_stat, typ, lic, veh, col, days, block, ward, district, comm)]

# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     n_req, date, complete, n_stat, typ, lic, veh, col, days, block, ward, district, comm = element
     record = { 'Request_Number': n_req, 'Date': date, 'Completion_Date': complete, 'Complete': n_stat, 'Service_Request_Type': typ, 'License_Plate': lic, 'Vehicle_Make': veh, 'Vehicle_Color': col, 'Parked_Days': days, 'Street_Address': block, 'Ward': ward, 'District': district, 'Community_Area': comm }
     return [record] 

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM chicago_abandoned_2001.abandoned_data_aggregated LIMIT 100000'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection
    request_pcoll = query_results | 'Extract Request' >> beam.ParDo(RequestNumFn())

    # write PCollection to log file
    request_pcoll | 'Write to log 2' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = request_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':chicago_abandoned_2001.Abandoned_Data_APR_Revised'
    table_schema = 'Request_Number:INTEGER,Date:DATE,Completion_Date:DATE,Complete:BOOL,Service_Request_Type:STRING,License_Plate:STRING,Vehicle_Make:STRING,Vehicle_Color:STRING,Parked_Days:INTEGER,Street_Address:STRING,Ward:INTEGER,District:INTEGER,Community_Area:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

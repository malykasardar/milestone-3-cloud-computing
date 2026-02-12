import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def preprocess(element):
    # Parse the JSON string into a dictionary
    data = json.loads(element.decode('utf-8'))
    
    # FILTER: Drop records if any measurement is 'nan' or missing
    fields = ['temperature', 'pressure', 'humidity']
    if any(data.get(f) == 'nan' or data.get(f) is None for f in fields):
        return None 

    # CONVERT: Pressure from kPa to psi (P/6.895)
    data['pressure'] = float(data['pressure']) / 6.895
    
    # CONVERT: Temperature from C to F (C * 1.8 + 32)
    data['temperature'] = (float(data['temperature']) * 1.8) + 32
    
    # Return as encoded JSON for Pub/Sub
    return json.dumps(data).encode('utf-8')

def run():
    project = "deft-racer-485415-t6"
    input_topic = f"projects/{project}/topics/M1Design"
    output_topic = f"projects/{project}/topics/ProcessedReadings"

    options = PipelineOptions(streaming=True, project=project, region="northamerica-northeast2")
    
    with beam.Pipeline(options=options) as p:
        (p 
         | "Read" >> beam.io.ReadFromPubSub(topic=input_topic)
         | "Process" >> beam.Map(preprocess)
         | "Filter Nulls" >> beam.Filter(lambda x: x is not None)
         | "Write" >> beam.io.WriteToPubSub(topic=output_topic)
        )

if __name__ == "__main__":
    run()
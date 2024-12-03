import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions

# Define a custom PipelineOptions class to handle custom parameters like input and output paths
class WordCountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', required=True, help='Input file to process.')
        parser.add_argument('--output_path', required=True, help='Output file to write results to.')

def run():
    # Initialize the custom PipelineOptions class to parse command-line arguments
    options = WordCountOptions()

    # Parse the input and output paths from the command-line arguments
    input_path = options.input_path
    output_path = options.output_path

    # Define Google Cloud-specific options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'qwiklabs-gcp-01-bb6a5c0f0c39'  # Replace with your project ID
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = 'gs://qwiklabs-gcp-01-bb6a5c0f0c39-bucket/staging/'  # Replace with your GCS path
    google_cloud_options.temp_location = 'gs://qwiklabs-gcp-01-bb6a5c0f0c39-bucket/temp/'  # Replace with your GCS path

    # Set the runner to DataflowRunner
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'

    # Configure worker options
    worker_options = options.view_as(WorkerOptions)
    worker_options.autoscaling_algorithm = "THROUGHPUT_BASED"
    worker_options.machine_type = "n1-standard-1"  # Adjust based on expected workload

    # Define the Apache Beam pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read Input File" >> beam.io.ReadFromText(input_path)
            | "Split into Words" >> beam.FlatMap(lambda line: line.split())
            | "Pair with One" >> beam.Map(lambda word: (word, 1))
            | "Count Words" >> beam.CombinePerKey(sum)
            | "Format Results" >> beam.MapTuple(lambda word, count: f"{word}: {count}")
            | "Write to Output File" >> beam.io.WriteToText(output_path, file_name_suffix=".txt", num_shards=1)
        )

if __name__ == "__main__":
    run()

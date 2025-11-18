from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class FormatUserData(beam.DoFn):
    """Transform to reformat user data to required format"""
    
    def process(self, element):
        # Skip the header row
        if element.startswith('User,Gender'):
            yield 'User;Gender;Age;Address;Date joined'
            return
        
        # Split the CSV line
        parts = element.split(',')
        
        if len(parts) >= 5:
            user = parts[0]
            gender = parts[1].lower()  # Convert to lowercase
            age = parts[2]
            address = parts[3].strip()  # Remove leading space
            date_joined = parts[4]
            
            # Reformat address from "City-State-Zip" to "City,State,Zip"
            address_formatted = address.replace('-', ',')
            
            # Reformat date from "YYYY/MM/DD" to "YYYY-MM-DD"
            date_formatted = date_joined.replace('/', '-')
            
            # Create output with semicolon separator
            output = f"{user};{gender};{age};{address_formatted};{date_formatted}"
            
            yield output


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Create the pipeline
    p = beam.Pipeline(options=pipeline_options)

    # Read, transform, and write
    output = (
        p
        | 'read' >> ReadFromText(known_args.input)
        | 'format' >> beam.ParDo(FormatUserData())
        | 'write' >> WriteToText(known_args.output))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

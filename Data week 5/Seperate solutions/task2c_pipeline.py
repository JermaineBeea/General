from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ExtractState(beam.DoFn):
    """Extract state from user address data"""
    
    def process(self, element):
        # Skip the header row
        if element.startswith('User,Gender'):
            return
        
        # Split the CSV line
        parts = element.split(',')
        
        if len(parts) >= 4:
            # Address format is: " City-State-Zip"
            address = parts[3].strip()
            # Split address by hyphen to get City-State-Zip
            address_parts = address.split('-')
            
            if len(address_parts) >= 2:
                state = address_parts[1]
                yield state


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

    # Read, extract states, count per state, and write
    output = (
        p
        | 'read' >> ReadFromText(known_args.input)
        | 'extract_state' >> beam.ParDo(ExtractState())
        | 'count_per_state' >> beam.combiners.Count.PerElement()
        | 'write' >> WriteToText(known_args.output))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

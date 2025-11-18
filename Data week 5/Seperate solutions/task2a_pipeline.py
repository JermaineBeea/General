from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ExtractGender(beam.DoFn):
    """Extract gender from user data"""
    
    def process(self, element):
        # Skip the header row
        if element.startswith('User,Gender'):
            return
        
        # Split the CSV line
        parts = element.split(',')
        
        if len(parts) >= 2:
            gender = parts[1]
            yield gender


class CalculateGenderPercentages(beam.DoFn):
    """Calculate gender percentages from counts"""
    
    def process(self, element):
        gender_counts = dict(element[1])
        total = sum(gender_counts.values())
        
        percentages = {}
        for gender, count in gender_counts.items():
            percentages[gender] = round(count / total, 2)
        
        yield ('gender', percentages)


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

    # Read, count genders, calculate percentages, and write
    output = (
        p
        | 'read' >> ReadFromText(known_args.input)
        | 'extract_gender' >> beam.ParDo(ExtractGender())
        | 'count_per_gender' >> beam.combiners.Count.PerElement()
        | 'combine_all' >> beam.combiners.ToDict()
        | 'calculate_percentages' >> beam.ParDo(CalculateGenderPercentages())
        | 'write' >> WriteToText(known_args.output))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

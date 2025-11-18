from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class GetDate(beam.DoFn):
    def process(self, element):
        if element.startswith('User,Gender'):
            return
        
        parts = element.split(',')
        
        if len(parts) >= 5:
            date = parts[4]
            # convert the date format
            date = date.replace('/', '-')
            yield date

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

    p = beam.Pipeline(options=pipeline_options)

    output = (
        p
        | 'read' >> ReadFromText(known_args.input)
        | 'get_date' >> beam.ParDo(GetDate())
        | 'count' >> beam.combiners.Count.PerElement()
        | 'write' >> WriteToText(known_args.output))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class FormatUserData(beam.DoFn):
    def process(self, element):
        # check if its the header
        if element.startswith('User,Gender'):
            yield 'User;Gender;Age;Address;Date joined'
            return
        
        parts = element.split(',')
        
        if len(parts) >= 5:
            user = parts[0]
            gender = parts[1].lower()
            age = parts[2]
            address = parts[3].strip()
            date = parts[4]
            
            # change the address format
            address = address.replace('-', ',')
            
            # change date format
            date = date.replace('/', '-')
            
            # put it all together with semicolons
            result = f"{user};{gender};{age};{address};{date}"
            
            yield result

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
        | 'format' >> beam.ParDo(FormatUserData())
        | 'write' >> WriteToText(known_args.output))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
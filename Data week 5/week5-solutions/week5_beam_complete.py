from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime


# ============================================================================
# TASK 1: Basic Transformations - Transform user data to marketing format
# ============================================================================

class FormatUserData(beam.DoFn):
    """
    Transform user data to marketing format:
    - Split CSV into fields
    - Convert gender to lowercase
    - Reformat address from "City-State-Zip" to "City,State,Zip"
    - Convert date from YYYY/MM/DD to YYYY-MM-DD
    """
    def process(self, element):
        # Skip header row
        if element.startswith('User,'):
            yield 'User;Gender;Age;Address;Date joined'
            return
        
        # Split the CSV line
        parts = element.split(',')
        
        if len(parts) >= 5:
            user = parts[0].strip()
            gender = parts[1].strip().lower()
            age = parts[2].strip()
            
            # Address format: " City-State-Zip" -> "City,State,Zip"
            address = parts[3].strip()
            address = address.replace('-', ',')
            
            # Date format: YYYY/MM/DD -> YYYY-MM-DD
            date_joined = parts[4].strip()
            date_joined = date_joined.replace('/', '-')
            
            # Format output with semicolon delimiter
            output = f"{user};{gender};{age};{address};{date_joined}"
            yield output


# ============================================================================
# TASK 2.1: Gender Aggregation - Calculate % split between genders
# ============================================================================

class ExtractGender(beam.DoFn):
    """Extract gender from user data"""
    def process(self, element):
        # Skip header row
        if element.startswith('User,'):
            return
        
        parts = element.split(',')
        if len(parts) >= 2:
            gender = parts[1].strip()
            yield gender


class CalculateGenderPercentages(beam.DoFn):
    """Calculate percentage split for each gender"""
    def process(self, element):
        gender_counts = element
        total = sum(gender_counts.values())
        
        # Calculate percentages rounded to 2 decimal places
        percentages = {
            gender: round(count / total, 2)
            for gender, count in gender_counts.items()
        }
        
        yield ('gender', percentages)


# ============================================================================
# TASK 2.2: Customer Totals by Join Date
# ============================================================================

class ExtractJoinDate(beam.DoFn):
    """Extract and format join date from user data"""
    def process(self, element):
        # Skip header row
        if element.startswith('User,'):
            return
        
        parts = element.split(',')
        if len(parts) >= 5:
            # Get date and convert to YYYY-MM-DD format
            date_joined = parts[4].strip().replace('/', '-')
            yield date_joined


# ============================================================================
# TASK 2.3: Customer Totals by State
# ============================================================================

class ExtractState(beam.DoFn):
    """Extract state from address field"""
    def process(self, element):
        # Skip header row
        if element.startswith('User,'):
            return
        
        parts = element.split(',')
        if len(parts) >= 4:
            # Address format: " City-State-Zip"
            address = parts[3].strip()
            # Split on hyphen and get the state (middle part)
            address_parts = address.split('-')
            if len(address_parts) >= 2:
                state = address_parts[1]
                yield state


# ============================================================================
# MAIN PIPELINE FUNCTIONS
# ============================================================================

def run_task1(input_file, output_file):
    """
    Task 1: Format user data to marketing format
    """
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read users' >> ReadFromText(input_file)
         | 'Format data' >> beam.ParDo(FormatUserData())
         | 'Write formatted' >> WriteToText(output_file))


def run_task2_gender(input_file, output_file):
    """
    Task 2.1: Calculate gender percentage split
    """
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read users' >> ReadFromText(input_file)
         | 'Extract gender' >> beam.ParDo(ExtractGender())
         | 'Count by gender' >> beam.combiners.Count.PerElement()
         | 'Combine to dict' >> beam.combiners.ToDict()
         | 'Calculate percentages' >> beam.ParDo(CalculateGenderPercentages())
         | 'Write gender totals' >> WriteToText(output_file))


def run_task2_dates(input_file, output_file):
    """
    Task 2.2: Count customers by join date
    """
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read users' >> ReadFromText(input_file)
         | 'Extract date' >> beam.ParDo(ExtractJoinDate())
         | 'Count by date' >> beam.combiners.Count.PerElement()
         | 'Write date totals' >> WriteToText(output_file))


def run_task2_states(input_file, output_file):
    """
    Task 2.3: Count customers by state
    """
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read users' >> ReadFromText(input_file)
         | 'Extract state' >> beam.ParDo(ExtractState())
         | 'Count by state' >> beam.combiners.Count.PerElement()
         | 'Write state totals' >> WriteToText(output_file))


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def run(argv=None, save_main_session=True):
    """
    Main function to run the appropriate pipeline based on task argument
    """
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
    parser.add_argument(
        '--task',
        dest='task',
        default='task1',
        choices=['task1', 'task2_gender', 'task2_dates', 'task2_states'],
        help='Which task to run: task1, task2_gender, task2_dates, or task2_states')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Run the appropriate task
    if known_args.task == 'task1':
        run_task1(known_args.input, known_args.output)
    elif known_args.task == 'task2_gender':
        run_task2_gender(known_args.input, known_args.output)
    elif known_args.task == 'task2_dates':
        run_task2_dates(known_args.input, known_args.output)
    elif known_args.task == 'task2_states':
        run_task2_states(known_args.input, known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

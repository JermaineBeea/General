# ============================================================================
# Week 5 Apache Beam Exercise - Interactive Notebook Version
# ============================================================================
# This notebook is designed to be run cell-by-cell in Jupyter/Colab
# Each cell can be executed independently to test and understand the transformations

# ============================================================================
# CELL 1: Setup and Installation
# ============================================================================

# Run and print a shell command
def run(cmd):
    print('>> {}'.format(cmd))
    get_ipython().system(cmd)
    print('')

run('pip install --upgrade pip')
run('pip install --quiet apache-beam')

# ============================================================================
# CELL 2: Import Libraries
# ============================================================================

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
import re
from datetime import datetime

# ============================================================================
# CELL 3: Helper Function for Interactive Testing
# ============================================================================

def myprint(x):
    """Custom print function for viewing pipeline outputs"""
    print('{}'.format(x))
    return x

# ============================================================================
# CELL 4: Upload or Create Test Data
# ============================================================================

# If in Colab, upload the users.csv and orders.csv files
# Or create sample data for testing:

sample_data = """User,Gender,Age,Address,Date joined
Amy Sullivan,Female,20, Westlake-OH-44145,2020/08/31
Paige Dixon,Female,43, Hicksville-NY-11801,2020/03/22
Valeria Hamilton,Female,54, Meriden-CT-6450,2020/05/28
Justin Robinson,Male,35, Auburn-NY-13021,2020/04/04
Frederick Barnes,Male,48, Ridgewood-NJ-7450,2020/05/12"""

# Write sample data to file
with open('sample_users.csv', 'w') as f:
    f.write(sample_data)

print("Sample data created!")

# ============================================================================
# CELL 5: TASK 1 - Define FormatUserData Transform
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
# CELL 6: TASK 1 - Test the Transform Interactively
# ============================================================================

print("Testing FormatUserData transform:\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('sample_users.csv')
        | 'Format data' >> beam.ParDo(FormatUserData())
        | 'Print' >> beam.Map(myprint)
    )

# ============================================================================
# CELL 7: TASK 1 - Run Full Pipeline and Save to File
# ============================================================================

print("\nRunning Task 1 - Saving to marketing_format.txt\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('users.csv')  # Use your actual file
        | 'Format data' >> beam.ParDo(FormatUserData())
        | 'Write' >> WriteToText('outputs/marketing_format.txt')
    )

print("✓ Task 1 complete! Check outputs/marketing_format.txt-00000-of-00001")

# ============================================================================
# CELL 8: TASK 2.1 - Gender Aggregation Classes
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
# CELL 9: TASK 2.1 - Test Gender Extraction
# ============================================================================

print("Testing gender extraction:\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('sample_users.csv')
        | 'Extract gender' >> beam.ParDo(ExtractGender())
        | 'Print' >> beam.Map(myprint)
    )

# ============================================================================
# CELL 10: TASK 2.1 - Run Gender Aggregation Pipeline
# ============================================================================

print("\nRunning Task 2.1 - Gender percentage split\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('users.csv')  # Use your actual file
        | 'Extract gender' >> beam.ParDo(ExtractGender())
        | 'Count by gender' >> beam.combiners.Count.PerElement()
        | 'Combine to dict' >> beam.combiners.ToDict()
        | 'Calculate percentages' >> beam.ParDo(CalculateGenderPercentages())
        | 'Print result' >> beam.Map(myprint)
        | 'Write' >> WriteToText('outputs/gender_totals.txt')
    )

print("✓ Task 2.1 complete! Check outputs/gender_totals.txt-00000-of-00001")

# ============================================================================
# CELL 11: TASK 2.2 - Date Aggregation Class
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
# CELL 12: TASK 2.2 - Test Date Extraction
# ============================================================================

print("Testing date extraction:\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('sample_users.csv')
        | 'Extract date' >> beam.ParDo(ExtractJoinDate())
        | 'Print' >> beam.Map(myprint)
    )

# ============================================================================
# CELL 13: TASK 2.2 - Run Date Aggregation Pipeline
# ============================================================================

print("\nRunning Task 2.2 - Customer count by join date\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('users.csv')  # Use your actual file
        | 'Extract date' >> beam.ParDo(ExtractJoinDate())
        | 'Count by date' >> beam.combiners.Count.PerElement()
        | 'Print result' >> beam.Map(myprint)
        | 'Write' >> WriteToText('outputs/customer_totals.txt')
    )

print("✓ Task 2.2 complete! Check outputs/customer_totals.txt-00000-of-00001")

# ============================================================================
# CELL 14: TASK 2.3 - State Aggregation Class
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
# CELL 15: TASK 2.3 - Test State Extraction
# ============================================================================

print("Testing state extraction:\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('sample_users.csv')
        | 'Extract state' >> beam.ParDo(ExtractState())
        | 'Print' >> beam.Map(myprint)
    )

# ============================================================================
# CELL 16: TASK 2.3 - Run State Aggregation Pipeline
# ============================================================================

print("\nRunning Task 2.3 - Customer count by state\n")

with beam.Pipeline() as pipeline:
    (pipeline 
        | 'Read users' >> ReadFromText('users.csv')  # Use your actual file
        | 'Extract state' >> beam.ParDo(ExtractState())
        | 'Count by state' >> beam.combiners.Count.PerElement()
        | 'Print result' >> beam.Map(myprint)
        | 'Write' >> WriteToText('outputs/state_totals.txt')
    )

print("✓ Task 2.3 complete! Check outputs/state_totals.txt-00000-of-00001")

# ============================================================================
# CELL 17: View Output Files
# ============================================================================

# View the results
print("=== Marketing Format Output (first 5 lines) ===")
get_ipython().system('head -5 outputs/marketing_format.txt-00000-of-00001')

print("\n=== Gender Totals ===")
get_ipython().system('cat outputs/gender_totals.txt-00000-of-00001')

print("\n=== Customer Totals by Date (first 10) ===")
get_ipython().system('head -10 outputs/customer_totals.txt-00000-of-00001')

print("\n=== State Totals (first 10) ===")
get_ipython().system('head -10 outputs/state_totals.txt-00000-of-00001')

# ============================================================================
# CELL 18: Bonus - Combined Pipeline Example
# ============================================================================

print("\nBonus: Running all transforms in a single view\n")

with beam.Pipeline() as pipeline:
    # Read the input
    lines = pipeline | 'Read' >> ReadFromText('sample_users.csv')
    
    # Task 1: Format data
    formatted = lines | 'Format' >> beam.ParDo(FormatUserData())
    
    # Task 2.1: Gender analysis
    genders = (lines 
               | 'Extract gender' >> beam.ParDo(ExtractGender())
               | 'Count gender' >> beam.combiners.Count.PerElement())
    
    # Task 2.2: Date analysis
    dates = (lines 
             | 'Extract date' >> beam.ParDo(ExtractJoinDate())
             | 'Count dates' >> beam.combiners.Count.PerElement())
    
    # Task 2.3: State analysis
    states = (lines 
              | 'Extract state' >> beam.ParDo(ExtractState())
              | 'Count states' >> beam.combiners.Count.PerElement())
    
    # Print all results
    formatted | 'Print formatted' >> beam.Map(lambda x: print(f"Formatted: {x}"))
    genders | 'Print genders' >> beam.Map(lambda x: print(f"Gender: {x}"))
    dates | 'Print dates' >> beam.Map(lambda x: print(f"Date: {x}"))
    states | 'Print states' >> beam.Map(lambda x: print(f"State: {x}"))

print("\n✓ All pipelines executed successfully!")

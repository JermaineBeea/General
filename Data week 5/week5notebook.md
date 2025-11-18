Interactive Beam¶

In this notebook, we set up your development environment and work through a simple example using the DirectRunner. You can explore other runners with the Beam Capatibility Matrix.

The expectation is that this notebook will help you explore the tutorial in a more interactive way.

To learn more about Colab, see Welcome to Colaboratory!.


Setup

First, you need to set up your environment, which includes installing apache-beam and downloading a text file from Cloud Storage to your local file system. We are using this file to test your pipeline.

# Run and print a shell command.
def run(cmd):
  print('>> {}'.format(cmd))
  !{cmd}
  print('')

run('pip install --upgrade pip')

# Install apache-beam.
run('pip install --quiet apache-beam')

# Install pandas.
run('pip install --quiet pandas as pd')

# Copy the input file into the local file system.
run('mkdir -p data')
run('gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt data/')

! wc -l data/kinglear.txt

! head -3 data/kinglear.txt

import apache_beam as beam
import re

inputs_pattern = 'data/*'
outputs_prefix = 'outputs/part'

How to interactively work with Beam

Here is an example of how to work iteratively with beam in order to understand what is happening in your pipeline.

Firstly, reduce the size of the King Lear file to be manageable


! head -10 data/kinglear.txt > data/small.txt
! wc -l data/small.txt

Create a custom print function (the python3 function print is supposed to work but we define our own here). Then it is possible to see what you are doing to the file.

But something is wrong... why is it printing twice, see SO

def myprint(x):
  print('{}'.format(x))

with beam.Pipeline() as pipeline:
  (pipeline 
      | 'Read lines' >> beam.io.ReadFromText('data/small.txt')
      | "print" >> beam.Map(myprint)
  )

result = pipeline.run()
result.wait_until_finish()

Now, let's break split each line on spaces and get the words out.


with beam.Pipeline() as pipeline:
  (pipeline 
      | 'Read lines' >> beam.io.ReadFromText('data/small.txt')
      | 'get words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line))
      | "print" >> beam.Map(myprint)
  )


Recall that flatMaps typically act on something (a function, iterable or variable) and apply a function to that something to produce a list of elements. See this great example of how FlatMap works in Beam, and this answer on SO for a simple explanation.

In the case above, we applied an anonymous function (lambda function) to a line. We can define it explicitly if you prefer a more conventional syntax


def my_line_split_func(line):
  return re.findall(r"[a-zA-Z']+", line)

with beam.Pipeline() as pipeline:
  (pipeline 
      | 'Read lines' >> beam.io.ReadFromText('data/small.txt')
      | 'get words' >> beam.FlatMap(my_line_split_func)
      | "print" >> beam.Map(myprint)
  )

Tutorial

! echo -e 'r1c1,r1c2,2020/03/05\nr2c1,r2c2,2020/03/23' > data/play.csv



class Transform(beam.DoFn):

  # Use classes to perform transformations on your PCollections
  # Yield or return the element(s) needed as input for the next transform
  def process(self, element):
    yield element


with beam.Pipeline() as pipeline:
  (pipeline 
      | 'Read lines' >> beam.io.ReadFromText('data/play.csv')
      | 'format line' >> beam.ParDo(Transform())
      | "print" >> beam.Map(myprint)
  )


result.wait_until_finish()
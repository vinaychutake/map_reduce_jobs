# Find the lines containing a certain word

from mrjob.job import MRJob
import re

# Regular expression that finds all the words in a String
WORD_REGEX = re.compile(r"\b\w+\b")


# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # Our mapper takes a fragment of text as an input and produces a list of (key, value) 
    # The key is a word and the value is 1, indicating one instance of the word has been read
    def mapper(self, _, line):
        if re.search("London", line):
            yield(line, None)


# 
if __name__ == '__main__':
    MyMapReduce.run()

"""
python findwordline.py input > out.txt
"""
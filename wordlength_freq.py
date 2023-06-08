# find the frequency of the word length

from mrjob.job import MRJob
import re

# Regular expression that finds all the words in a String
WORD_REGEX = re.compile(r"\b\w+\b")


# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # We don't need a key and the value is 1, indicating one word has been read
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield(len(word) , 1)

    # One single reducer takes all (key, value) produced by the mappers
    # The final value is the total number of words
    def reducer(self, length, counts):
        yield(length, sum(counts))


# 
if __name__ == '__main__':
    MyMapReduce.run()

""" Command:
python wordlength_freq.py full > out.txt
"""
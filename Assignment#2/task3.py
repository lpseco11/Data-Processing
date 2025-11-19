from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class CounteWordsSorted(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_word_mapper, reducer=self.count_words_reducer),\
            MRStep(mapper=self.sort_words_mapper, reducer=self.sort_words_reducer),\
        ]

    def get_word_mapper(self, _, line):
        words = line.translate(str.maketrans('', '', string.punctuation)).split()
        for word in words:
            yield word.lower(), 1


    def count_words_reducer(self,word,count):
        yield word, sum(count)
        
    def sort_words_mapper(self, word, count):
        yield None, (count, word)


    def sort_words_reducer(self, _, count_word_pairs):
        sorted_pairs = sorted(count_word_pairs, reverse=True, key=lambda x: x[0])
        for count, word in sorted_pairs:
            yield word, count

if __name__ == '__main__':
    CounteWordsSorted.run()
from mrjob.job import MRJob
from mrjob.step import MRStep

class FriendsByAge(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_friends_mapper, reducer=self.count_friends_reducer),
            MRStep(reducer=self.output_reducer)
        ]

    
    def get_friends_mapper(self, _, line):
        _, _, age, num_friends = line.split(',')
        yield age, int(num_friends)


    def count_friends_reducer(self, age, num_friends):
        total, num_elements = 0, 0
        for x in num_friends:
            total += x
            num_elements += 1
        
        average = total / num_elements
        yield age, average


    def output_reducer(self, age, averages):
        for average in averages:
            yield int(age), average

if __name__ == '__main__':
    FriendsByAge.run()
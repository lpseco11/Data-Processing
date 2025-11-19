from mrjob.job import MRJob
from mrjob.step import MRStep


class MinTemperatureByCapital(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_temperature_mapper, reducer=self.minimum_temperature_reducer),
        ]

    def get_temperature_mapper(self, _, line):
        station, _, type, temperature,_,_,_,_ = line.split(',')
        capital = ''.join(filter(str.isalpha, station))
        if type == "TMIN":
            yield capital, int(temperature)

    def minimum_temperature_reducer(self, capital, temperature):
        yield capital, min(temperature)
        
if __name__ == '__main__':
    MinTemperatureByCapital.run()
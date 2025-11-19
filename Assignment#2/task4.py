from mrjob.job import MRJob
from mrjob.step import MRStep

class SortamountByCustomer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_customer_amount_mapper, reducer=self.get_customer_amount_reducer),\
            MRStep(mapper=self.sort_customer_by_spent_amount_mapper, reducer=self.sort_customer_by_spent_amount_reducer),\
        ]

    def get_customer_amount_mapper(self, _, line):
        customer_id, _,amount = line.split(',')
        yield customer_id, float(amount)
        
    def get_customer_amount_reducer(self,customer, amount):
        yield customer, sum(amount) 

    def sort_customer_by_spent_amount_mapper(self,customer, amount):
        yield None, (amount, customer)
            
    def sort_customer_by_spent_amount_reducer(self, _, amount_customer_pairs):
        sorted_pairs = sorted(amount_customer_pairs, reverse=True, key=lambda x: x[0])
        for amount, customer in sorted_pairs:
            yield customer, amount

        
if __name__ == '__main__':
    SortamountByCustomer.run()
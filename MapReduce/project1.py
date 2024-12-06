from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


import math
import sys

class proj1(MRJob):  

    SORT_VALUES = True 

    def mapper(self, key, value):
        year_dict = {}
        data = value.split(",")
        year = data[0][:4]
        terms = data[1]
        real_terms = terms.split(" ")
        
        for term in real_terms:
            year_dict[term] = year_dict.get(term, 0) + 1
        
        # sys.stderr.write('this is a mapper log\n')
        yield year + "*" + "$", (1, 0)
        # sys.stderr.write(f'mapper output: {year}*$ : 1, 0\n')
        for term, count in year_dict.items():
            yield year + "*" + term, (count, 1)
        # sys.stderr.write(f'mapper output: {year}*{term} : {count}, 1\n')
        
    def combiner(self, key, values):
        sum_1 = 0
        sum_2 = 0
        for v in values:
            sum_1 += v[0]
            sum_2 += v[1]
        yield key, (sum_1, sum_2)

    def reducer_init(self):
        self.k = int(jobconf_from_env('myjob.settings.k'))
        self.top_k = {}
        self.B_IDF_1 = 0
        self.last_year = ""

    def reducer(self, key, values):
        # sys.stderr.write('this is a reducer log\n')
        # sys.stderr.write(f'reducer input: {key} : {values}\n')
        keys = key.split("*")
        year = keys[0]
        term = keys[1]

        if term == "$":
            # sys.stderr.write('this is a reducer log\n')
            # sys.stderr.write(f'if else 1: {key}\n')
            self.B_IDF_1 = 0
            for v in values:
                self.B_IDF_1 += v[0]
        else:
            A_TF = 0
            C_IDF_2 = 0
            for v in values:
                A_TF += v[0]
                C_IDF_2 += v[1]
            # get weight
            # sys.stderr.write('this is a reducer log\n')
            # sys.stderr.write(f'if else 2: {key}\n')
            # sys.stderr.write(f'if else 2: {self.B_IDF_1}\n')
            weight = A_TF * math.log10(self.B_IDF_1/C_IDF_2)
            
            if year in self.top_k:
                self.top_k[year].append((term, weight))
            else:
                if self.top_k:
                    # processing last year
                    last_weights = self.top_k[self.last_year]
                    new_weights = sorted(last_weights, key=lambda x: (-x[1], x[0]))[:self.k]
                    self.top_k[self.last_year] = new_weights

                # processing this year
                # Initialize for the new year
                self.top_k[year] = [(term, weight)]
                self.last_year = year

        # sys.stderr.write(f'reducer dict input1: {self.top_k}\n')

    def reducer_final(self):
        last_weights = self.top_k[self.last_year]
        new_weights = sorted(last_weights, key=lambda x: (-x[1], x[0]))[:self.k]
        self.top_k[self.last_year] = new_weights

        # sys.stderr.write(f'reducer dict input2: {self.top_k}\n')
        for year in self.top_k:
            for w in self.top_k[year]:
                result = f'{w[0]},{w[1]}'
                yield year, result

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer_init=self.reducer_init,
                reducer=self.reducer,
                reducer_final=self.reducer_final,
                jobconf= {
                        'mapreduce.map.output.key.field.separator': '*',
                        'partitioner': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
                        'mapreduce.partition.keypartitioner.options': '-k1,1',
                        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2'
                        }
                    )
                ]
                
if __name__ == '__main__':
    proj1.run()

from sys import maxsize
import numpy as np
from dataloading import data_generator

class TimeSeries(object):
    '''Implements a dataset containing timeseries data from multiple sources'''

    def __init__(self, datasource=[('BTCUSD','ticker')], t_start=0, t_end=maxsize):
        super().__init__()
        self.sources = datasource
        self.t_start = t_start
        self.t_end = t_end
        self.mark = 0
        self.data = {}
        for source in self.sources:
            self.load_datasource(source)
        self.match_sources()

    def load_datasource(self, source):
        '''Loads a datasource and stores it as n-dimensional numpy array'''
        pair, channel = source
        generator = data_generator(pair, channel, self.t_start, self.t_end)
        items = [eval(x) for  f in generator for x in f.split('\n')]
        self.data[source] = np.array(items)

    def match_sources(self):
        '''Matches the length of different source via their timestamps.
           Unimplemented, Assumes single source, batch size 1'''
        self.length = len(self.data[self.sources[0]])

    def __len__(self):
        return self.length

    def __getitem__(self, key):
        return {source: data[key] for source, data in self.data.items()}

    def __setitem__(self, key, value):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        if self.mark == self.length:
            self.mark == 0
            raise StopIteration
        ret = self.__getitem__(self.mark)
        self.mark += 1
        return ret

if __name__ == '__main__':
    x = TimeSeries()
    print('Number of Datapoints:{}'.format(x.length))
    i = 0
    for y in x:
        i += 1
        if i % 10000 == 0:
            print(i)
            print(y)

# next two lines are required if your default Java is > 1.8
# the precise location depends on your machine
import os


from pyspark import SparkContext

sc = SparkContext("local", "Simple App")


## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)


def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


class Mapper:
    def __init__(self):
        self.out = []

    # call in subclass to output a key-value pair
    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out


class Reducer:
    def __init__(self):
        self.out = []

    # call in subclass to output a key-value pair
    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out


def transform(input, mapper, reducer):
    return input.flatMap(lambda kd: mapper().map(kd[0], kd[1]).result()) \
        .groupByKey() \
        .flatMap(lambda kd: reducer().reduce(kd[0], kd[1]).result())


### WordCount

##Mapper implementation
class WCMapper(Mapper):
    # required
    def __init__(self):
        super().__init__()

    # provided by YOU
    def map(self, key, data):
        for x in data.split(" "):
            self.emit(x, 1)
        return self


##Reducer implementation
class WCReducer(Reducer):
    # required
    def __init__(self):
        super().__init__()

    # provided by YOU
    def reduce(self, key, datalist):
        self.emit(key, sum(datalist))
        return self


def wordcount(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
    result = transform(rdd, WCMapper, WCReducer)
    # passing class NOT object (which would be WCMapper() etc)
    finalise(result, outputFile)


# text.txt is also provided on Studium for testing.
# will not work if count.out already exists!!
wordcount(sc, "text.txt", "count.out")

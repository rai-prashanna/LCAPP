# next two lines are required if your default Java is > 1.8
# the precise location depends on your machine
import os
import unittest

os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import shutil
from pathlib import Path
from pyspark import SparkContext
import helper

## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    dirpath = Path(outputFile)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
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



##Mapper implementation
class SalesMapper(Mapper):
    # required
    def __init__(self):
        super().__init__()
    # provided by YOU
    def map(self, key, data):
        _, itemid, sale = data.split(',')
        self.emit(itemid,int(sale))
        return self
        
##Reducer implementation
class SalesReducer(Reducer):
    # required
    def __init__(self):
        super().__init__()
    # provided by YOU
    def reduce(self, key, datalist):
        self.emit(key, sum(datalist))
        return self

def wordcount(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
    result = transform(rdd, SalesMapper, SalesReducer)
    # passing class NOT object (which would be WCMapper() etc)
    finalise(result, outputFile)


# text.txt is also provided on Studium for testing.
# will not work if count.out already exists!!

def driver():
    sc = SparkContext("local", "Simple App")
    wordcount(sc, "sales_data/sales.csv", "assingment1.out")
    ItemCategoryMapper = helper.LoadItemIdItemName("sales_data/item_categories.csv")
    SalesbyName = helper.MapItemIDtoName(ItemCategoryMapper,"assingment1.out/part-00000")
    CategoryMapper = helper.LoadCategoryMapper("sales_data/categories.csv")
    salesofEachSubCategory = helper.SalesofEachSubCategory(SalesbyName, CategoryMapper)
    salesOfEachCategory = helper.salesOfEachCategory(salesofEachSubCategory, CategoryMapper)
    print(SalesbyName)
    print("***************************************************")
    print(salesofEachSubCategory)
    print("***************************************************")
    print(salesOfEachCategory)


driver()

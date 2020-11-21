#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col

import sys

if __name__ == '__main__':

    sc = SparkContext(appName="innerjointestapp")




    # creating the context
    sqlContext = SQLContext(sc)

    # reading the first csv file and store it in an RDD
    # Each line will be a element in the array returned by split.
    rdd1 = sc.textFile('s3://hemr1234/TB_policies_services_2020-11-10.csv'
                       ).map(lambda line: line.split(','))

    rdd1.collect()                   

    # removing the first row as it contains the header
    # mapPartitionsWithIndex index
    rdd1 = rdd1.mapPartitionsWithIndex(lambda idx, it: (islice(it, 1,None) if idx == 0 else it))

    # converting the RDD into a dataframe
    df1 = rdd1.toDF(['country', 'iso_numeric', 'g_whoregion', 'wrd_initial_test'])

    # dataframe which holds rows after replacing the 0's into null
    targetDf = df1.withColumn('wrd_initial_test', when(df1['wrd_initial_test'
                              ] == 0, 'null'
                              ).otherwise(df1['wrd_initial_test']))
    
    targetDf.show()

    df1WithoutNullVal = targetDf.filter(targetDf.wrd_initial_test != 'null')
    df1WithoutNullVal.show()

   
    df1WithoutNullVal.write.parquet('s3://hemr1234/test.parquet')

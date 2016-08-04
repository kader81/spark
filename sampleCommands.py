wordsDF = sqlContext.createDataFrame([('cat',), ('elephant',), ('rat',), ('rat',), ('cat', )], ['word'])
wordsDF.show()
print type(wordsDF)
wordsDF.printSchema()
# adding literal 's'
from pyspark.sql.functions import lit, concat

pluralDF = wordsDF.select(concat(wordsDF.word,lit('s')).alias("word"))
pluralDF.show()
from pyspark.sql.functions import length
pluralLengthsDF = pluralDF.select(length('word'))
pluralLengthsDF.show()
# count sample
wordCountsDF = (wordsDF.groupBy('word').count())
                
wordCountsDF.show()
from spark_notebook_helpers import printDataFrames

#This function returns all the DataFrames in the notebook and their corresponding column names.
printDataFrames(True)
# Finding unique words and a mean value
uniqueWordsCount = wordsDF.distinct().select('word').count()
print uniqueWordsCount
averageCount = wordCountsDF.groupBy().mean('count').first()[0]
                
print averageCount
# sample function
def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('word').count()

wordCount(wordsDF).show()

# sample for removing punctuation
from pyspark.sql.functions import regexp_replace, trim, col, lower
def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return lower(trim(regexp_replace(column,'[^a-zA-Z0-9\n ]', ''))).alias('sentence')

sentenceDF = sqlContext.createDataFrame([('Hi, you!',),
                                         (' No under_score!',),
                                         (' *      Remove punctuation then spaces  * ',),(" The Elephant's 4 cats. ",)], ['sentence'])
sentenceDF.show(truncate=False)
(sentenceDF
 .select(removePunctuation(col('sentence')))
 .show(truncate=False))
 
 # loading text file 
 
 fileName = ""

shakespeareDF = sqlContext.read.text(fileName).select(removePunctuation(col('value')))
shakespeareDF.show(15, truncate=False)

# sample for splitting lines to words and making words as seperate rows in data frames

from pyspark.sql.functions import split, explode
shakeWordsDF1 = shakespeareDF.select(explode(split(shakespeareDF.sentence,' ')).alias('word'))
shakeWordsDF = shakeWordsDF1.where(shakeWordsDF1.word !="")
shakeWordsDF.show()
shakeWordsDFCount = shakeWordsDF.count()
print shakeWordsDFCount

from pyspark.sql.functions import desc
topWordsAndCountsDF = wordCount(shakeWordsDF).orderBy(desc("count"))
topWordsAndCountsDF.show()

# sample for creating collection of few records 
from faker import Factory
fake = Factory.create()
fake.seed(4321)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return (name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1)
def repeat(times, func, *args, **kwargs):
    for _ in xrange(times):
        yield func(*args, **kwargs)
data = list(repeat(10000, fake_entry))

# create data frame
dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))
print 'type of dataDF: {0}'.format(type(dataDF))
dataDF.printSchema()
dataDF.show(5)

# getting partitions

dataDF.rdd.getNumPartitions()

# distinct wit select and explain functions 

newDF = dataDF.distinct().select('*')
newDF.explain(True)

# subtract  values with alias
subDF = dataDF.select('last_name', 'first_name', 'ssn', 'occupation', (dataDF.age - 1).alias('age'))

results = subDF.collect()
print results

# using filter

filteredDF = subDF.filter(subDF.age < 10)
filteredDF.show(truncate=False)
filteredDF.count()

# lambda functions

from pyspark.sql.types import BooleanType
less_ten = udf(lambda s: s < 10, BooleanType())
lambdaDF = subDF.filter(less_ten(subDF.age))
lambdaDF.show()
lambdaDF.count()

even = udf(lambda s: s % 2 == 0, BooleanType())
evenDF = lambdaDF.filter(even(lambdaDF.age))
evenDF.show()
evenDF.count()

# orderBy , distinct


dataDF.orderBy(dataDF['age'])  # sort by age in ascending order; returns a new DataFrame
dataDF.orderBy(dataDF.last_name.desc()) # sort by last name in descending order

print dataDF.count()
print dataDF.distinct().count()

tempDF = sqlContext.createDataFrame([("Joe", 1), ("Joe", 1), ("Anna", 15), ("Anna", 12), ("Ravi", 5)], ('name', 'score'))
tempDF.show()
display(tempDF.orderBy(tempDF.score.desc()))
tempDF.distinct().show()

print dataDF.count()
print dataDF.dropDuplicates(['first_name', 'last_name']).count()

# drop
dataDF.drop('occupation').drop('age').show()

# groupBy
dataDF.groupBy('occupation').count().show(truncate=False)
dataDF.groupBy().avg('age').show(truncate=False)

# sampling 
sampledDF = dataDF.sample(withReplacement=False, fraction=0.10)
print sampledDF.count()
sampledDF.show()

# Cache the DataFrame
filteredDF.cache()
# Trigger an action
print filteredDF.count()
# Check if it is cached
print filteredDF.is_cached

# If we are done with the DataFrame we can unpersist it so that its memory can be reclaimed
filteredDF.unpersist()
# Check if it is cached
print filteredDF.is_cached




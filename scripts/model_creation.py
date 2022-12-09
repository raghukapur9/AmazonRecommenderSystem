from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import expr, col, length
from pyspark.ml.feature import Tokenizer, SQLTransformer, StopWordsRemover,\
    CountVectorizer, StringIndexer, IDF, VectorAssembler, NGram
from pyspark.ml.linalg import Vector
from pyspark.ml.classification import NaiveBayes, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel

import sys
import json

def get_observation_schema():
    '''
        - defining the df schema
    '''
    comments_schema = types.StructType([
        types.StructField('overall', types.FloatType()),
        types.StructField('reviewText', types.StringType()),
        types.StructField('summary', types.StringType())
    ])

    return comments_schema

def remove_non_ascii_chars(review):
    return review.encode('ascii', 'ignore').decode('ascii')

@functions.udf(returnType=types.StringType())
def preprocess_reviews(review):
    non_ascii_reviews = remove_non_ascii_chars(review)
    return non_ascii_reviews
    

def main(sc, inputs, output):
    ### Define data frame schema
    review_schema = get_observation_schema()

    ### Read inputs using command line arguments and create a df with the defined schema
    review_schema_df = spark.read.format("s3selectJSON").schema(review_schema).options(compression='gzip').load(inputs)

    review_schema_df = review_schema_df.filter(review_schema_df.reviewText.isNotNull())
    review_schema_df = review_schema_df.withColumn("reviewText",functions.lower(functions.col("reviewText")))

    review_schema_df = review_schema_df.withColumn(
        "processedReviewText",
        preprocess_reviews(review_schema_df.reviewText)
    )

    review_schema_df_with_sentiment = review_schema_df.select(col('*'), expr("CASE WHEN overall > 4 THEN 'Positive' " + 
        "WHEN overall >3.0 AND overall <=4 THEN 'Neutral' WHEN overall <=3.0 THEN 'Negative'" +
        "ELSE overall END").alias("sentiment")
    )

    review_schema_df_with_sentiment = review_schema_df_with_sentiment.withColumn(
        "length", length(review_schema_df_with_sentiment.processedReviewText)
    )

    (train, test) = review_schema_df_with_sentiment.randomSplit([0.8, 0.2])

    ## Feature Creation
    tokenizer = Tokenizer(inputCol="processedReviewText", outputCol = "tokenised_text")
    stop_word_remover = StopWordsRemover(inputCol = "tokenised_text", outputCol="tokenised_text_without_stop_words")
    ngram = NGram(n=2, inputCol="tokenised_text_without_stop_words", outputCol = "nGrams")
    count_vectorizer = CountVectorizer(inputCol = "nGrams", outputCol = "c_vectorized_review")
    idf = IDF(inputCol = "c_vectorized_review", outputCol = "tf_idf_reviews")
    sentiment_to_label = StringIndexer(inputCol = "sentiment", outputCol = "label")

    cleaned_data = VectorAssembler(inputCols=['tf_idf_reviews', 'length'], outputCol = 'features')

    naive_bayes = NaiveBayes()

    naive_bayes.setFeaturesCol('features')
    naive_bayes.setLabelCol('label')
    pipeline = Pipeline(stages =[
        sentiment_to_label,
        tokenizer,
        stop_word_remover,
        ngram,
        count_vectorizer,
        idf,
        cleaned_data,
        naive_bayes
    ])

    resultant_model = pipeline.fit(train)
    resultant_model_train = resultant_model.transform(train)

    evaluator = MulticlassClassificationEvaluator()
    accuracy = evaluator.evaluate(resultant_model_train)
    print(f"Accuracy on training data - {accuracy*100}")
    test_res = resultant_model.transform(test)
    accuracy = evaluator.evaluate(test_res)
    print(f"Accuracy on testing data - {accuracy*100}")

    resultant_model.write().overwrite().save(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Sentiment Analysis Model Creation').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(sc, inputs, output)
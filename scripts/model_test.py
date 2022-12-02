from os import truncate
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import expr, col, length
from pyspark.ml.feature import Tokenizer, SQLTransformer, StopWordsRemover,\
    CountVectorizer, StringIndexer, IDF, VectorAssembler, NGram
from pyspark.ml.linalg import Vector
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel

import sys
import json


def createDF(schema):
    '''
        Create dataframe with friday's and day after's weather 
    '''
    data = spark.createDataFrame([
        (6.0, "I was taken in by reviews that compared this book with The Leopard or promised a bildungsroman set in a family that is isolated and on the move.  Well...I didn't feel any parallel with The Leopard and the author overstuffs her story with so many events that isolation is lost in the opportunity for  so many subplots.  And who's moving - in any sense - in this novel?  Where's the \"bildung?\"  The denouement?  The dsillusionner?  I must have  skimmed the wrong paragraph.\nI do agree that parts of the novel are very  engaging.  They are.  And, like some other readers, I did enjoy the events  in Algeria.  This is the strongest part of the novel, perhaps because it is  not so deeply stuck in the protagonist's life that had the unfortunate  knack of boring me in the first-person!  (Yeah, that's what I mean.)  Overall, the novel is just a bit overdone.  I prefer a bildungsroman - if  that's what this should be - to show me a growth, not tell me all about the  all-too-interesting things that happened along the way.\nIf you're the  kind of person who can sit for hours and listen to a friend tell you  endless stories about herself that you can't help but suspect are just a  tad trumped up, then you might really dig into this novel.  Otherwise you  might have an experience more like mine which was one of detached interest.  For my part, I was relieved that I could close the book whenever I wanted  to quiet my jabbery friend.", "test"),
        (6.0,"I was taken in by reviews that compared this book with The Leopard or promised a bildungsroman set in a family that is isolated and on the move.  Well...I didn't feel any parallel with The Leopard and the author overstuffs her story with so many events that isolation is lost in the opportunity for  so many subplots.  And who's moving - in any sense - in this novel?  Where's the \"bildung?\"  The denouement?  The dsillusionner?  I must have  skimmed the wrong paragraph.\nI do agree that parts of the novel are very  engaging.  They are.  And, like some other readers, I did enjoy the events  in Algeria.  This is the strongest part of the novel, perhaps because it is  not so deeply stuck in the protagonist's life that had the unfortunate  knack of boring me in the first-person!  (Yeah, that's what I mean.)  Overall, the novel is just a bit overdone.  I prefer a bildungsroman - if  that's what this should be - to show me a growth, not tell me all about the  all-too-interesting things that happened along the way.\nIf you're the  kind of person who can sit for hours and listen to a friend tell you  endless stories about herself that you can't help but suspect are just a  tad trumped up, then you might really dig into this novel.  Otherwise you  might have an experience more like mine which was one of detached interest.  For my part, I was relieved that I could close the book whenever I wanted  to quiet my jabbery friend.", "A star is born"),
        (6.0,"Crows Can't Count, A.A. Fair\n\nMr. Harry Sharples came to the Cool & Lam Agency; he was a co-trustee for a trust fund that will terminate when the youngest beneficiary turns 25. One of the heirs, Robert Hockley, is wild and gambles. The other, Shirley Bruce, isn't but refuses to take more than the other. A large part of the estate is in Colombian emerald mines. A jeweler has an emerald pendant for sale. Sharples believes this was part of the estate given to Shirley Bruce. Sharples wants Lam to find out how the jeweler got the pendant, and if any pressure was used (Chapter 1). Chapter 2 describes the preliminary investigation. Lam sees Robert Cameron, the other co-trustee show up. Sharples is surprised and astonished to hear this news (Chapter 3). When Lam and Sharples go to visit Cameron, they find a dead body. They call the police from outside the house (Chapter 4). Sharples tells Sgt. Sam Buda that Cameron had no enemies (Chapter 5). The emerald pendant was there. The police investigate the jeweler and the stockbroker who was visited by Robert Cameron earlier that day (Chapter 8).\n\nLam visits Dona Grafton, who minded Cameron's crow when Cameron left the country (Chapter 11). An anonymous donor sent Dona a box of chocolates. Lam inspects the crow's nest. Lam learns that Robert Hockley was getting a passport and decides to follow him (Chapter 12). The police bring Lam to Sharples' residence. His office is disordered as from a struggle, and Sharples is missing (Chapter 15). Lam meets George Prenter on the airplane, and hears of the wonderful life and climate in Medellin Colombia (Chapter 16). A visitor surprises Lam and we learn the true facts behind the earlier events (Chapter 17). Lam learns that someone else has been killed (Chapter 19).\n\nLam and Cool get a telephone call about a prison escape. They will be guarded (Chapter 21). Lam and \"the delightful Senora Cool\" will be allowed to return to California as soon as possible (Chapter 22). Then Lam visits Dona Grafton and asks questions (Chapter 23). There is amazing testimony from a witness in Chapter 24. Lam places a call to the police so they will get an interpreter and notary public. This written statement will convict a businessman of fraud, and affect the wealth in a trust. Lam explains the facts behind the scandal (Chapter 25). [Do you remember the story of King Solomon and the disputed child?] The pieces of the jigsaw puzzle fall into place, just as the apple falls close to its tree (Chapter 26). One benefit of this story is the description of the trip to Colombia.", "A star is born"),
        (6.0,"Fresh from Connecticut, Taylor Henning lands a dream job at a major movie studio. Okay, so she's not a Creative Executive whose job it is to read and recommend screenplays for the studios to produce. No... she's just an assistant to the assistant of the powerful Iris Whitaker, President of Production.\n\nWhen first assistant Kylie Arthur sabotages Taylor at the first opportunity she almost gets fired. That's when Taylor realizes that Hollywood is just like high school. If you're not in, no one wants to be your friend.\n\nIn order for Taylor to survive in this environment she enlists the help of Iris' sixteen year old daughter Quinn. Irritated to no end Quinn nevertheless takes on the challenge and begins texting Taylor \"rules\" to get ahead in the cut throat world of the movie business and amazingly enough her tips are working! That is until Quinn's next rule is to steal Kylie's boyfriend. Torn between doing the right thing and securing her real dream job Taylor has some tough decisions to make.\n\nI LOVE, LOVE, LOVED this book!! It was so much fun. Yes it did remind me a lot of The Devil Wears Prada but Taylor became a cherished character and I very much wanted her to succeed. I even loved the snarky Quinn. She was a spoiled brat but she had a good heart and I was rooting for her too. This is a fun breezy read that I recommend to all chick lit lovers.", "A star is born")
    ],schema)
    return data

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
    

def main(inputs, output):
    ### Define data frame schema
    review_schema = get_observation_schema()

    ### Read inputs using command line arguments and create a df with the defined schema
    review_schema_df = createDF(review_schema)
    
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

    review_schema_df_with_sentiment = review_schema_df_with_sentiment.withColumn("length", length(review_schema_df_with_sentiment.processedReviewText))

    model = PipelineModel.load(inputs)
    predictions = model.transform(review_schema_df_with_sentiment)
    predictions.show()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Sentiment Analysis Model Creation').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
from pyspark import SparkConf, SparkContext
import sys
import json

# add more functions as necessary
def split_review_data(line):
    review_details = json.loads(line)
    overall = None if "overall" not in review_details else review_details["overall"]
    reviewText = None if "reviewText" not in review_details else review_details["reviewText"]
    summary = None if "summary" not in review_details else review_details["summary"]
    return (overall, reviewText, summary)

def output_format(review_details):
    '''
        - format the rdd entry into the required format to store in the output file
    '''
    overall, reviewText, summary = review_details[0], review_details[1], review_details[2]
    formatted_value = json.dumps(
        {
            "overall": overall,
            "reviewText": reviewText,
            "summary": summary
        }
    )
    return formatted_value

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    subreddit = text.map(split_review_data)
    subreddit.map(output_format).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    # print(inputs)
    output = sys.argv[2]
    main(inputs, output)
# Amazon Recommender System

## Architecture Diagram
![Architecture Diagram](/images/ArchitectureDiagram.jpeg?raw=true "Architecture Diagram")

## Problem Statement
- Create a system which shows the trend of products using the reviews being scraped from the website.

## Data Information (For sentiment analysis training)
- Dataset link: https://nijianmo.github.io/amazon/index.html
- Dataset has 6,739,590 reviews in total
- Dataset size is 3.9 GB
- Data is present in csv format and the schema for each row is as follow
```
{
  "overall": 5,
  "vote": "67",
  "verified": true,
  "reviewTime": "09 18, 1999",
  "reviewerID": "AAP7PPBU72QFM",
  "asin": "0151004714",
  "style": {
    "Format:": " Hardcover"
  },
  "reviewerName": "D. C. Carrad",
  "reviewText": "This is the best novel I have read in 2 or 3 years.  It is everything that fiction should be -- beautifully written, engaging, well-plotted and structured.  It has several layers of meanings -- historical, family,  philosophical and more -- and blends them all skillfully and interestingly.  It makes the American grad student/writers' workshop \"my parents were  mean to me and then my professors were mean to me\" trivia look  childish and silly by comparison, as they are.\nAnyone who says this is an  adolescent girl's coming of age story is trivializing it.  Ignore them.  Read this book if you love literature.\nI was particularly impressed with  this young author's grasp of the meaning and texture of the lost world of  French Algeria in the 1950's and '60's...particularly poignant when read in  1999 from another ruined and abandoned French colony, amid the decaying  buildings of Phnom Penh...\nI hope the author will write many more books  and that her publishers will bring her first novel back into print -- I  want to read it.  Thank you, Ms. Messud, for writing such a wonderful work.",
  "summary": "A star is born",
  "unixReviewTime": 937612800
}
```


- The data fields are as follow

    | Field Name        | Data Type       | Description                                     |
    | :----             | :----           | :----                                           |
    | overall           | int             | Review rating given by the user                 |
    | vote              | string          | No. of votes given to the review                |
    | verified          | boolean         | If the user has actually bought the product     |
    | reviewTime        | string          | Time when the review was created                |
    | reviewerID        | string          | ID of the reviewer                              |
    | asin              | string          | Unique identifier of the product                |
    | style             | dictionary      | A dictionary of the product’s metadata          |
    | reviewerName      | string          | Name of the reviewer                            |
    | reviewText        | string          | Details of the review                           |
    | unixReviewTime    | int             | Unix timestamp for the review                   |
    | summary           | string          | Summary of the review                           |


- Important fields to be kept for creating the sentiment analysis Model
    - overall
    - reviewText
    - Summary

- Steps involved in pre-processing of the data
    - Divide the initial file into files with size 200MB
    - Upload the files to S3
    - Use Pyspark to read the data from S3 json files, and read only the required fields
    - Add a field to the dataset marked as “classification” as +ve, -ve and neutral
    - Text preprocessing:
        - Tokenization (breaking sentences into words)
        - Stopwords (filtering "the", "are", etc)
    - Use TfidfTransformer to normalize the freq of words in the review
    - Train the model and store the model weights

## Scraping Data Pipeline
- Decide a list of amazon electronic products (asin)
- Create a python script to scrape the reviews for these products after a fixed interval (Use AWS Lambda to run this script)
- Pass the data through a Queue
- Another script runs in the backend which reads these data streams from the queue and pass the data through the model to generate a sentiment prediction
- Store this prediction along with review time and sentiment in a datastore like cassandra or normal sql might also work

## Dashboard
- Read the data from the datastore
- Show graphs with trends about how the product’s rating has changed over a period of some time frame.

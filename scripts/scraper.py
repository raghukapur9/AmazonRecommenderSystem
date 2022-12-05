import requests
import sys
from bs4 import BeautifulSoup
from asin_helper import asin_to_price_and_reviews
import datetime
import json
import sqs_helper
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

HEADERS = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/90.0.4430.212 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})

def get_date(soup):
    date_list = []
    for item in soup.find_all("span", class_="a-size-base a-color-secondary review-date"):
        full_string = item.get_text() # text like 'Reviewed in Canada on May 11, 2022'
        date_words = full_string.split()[-3:]
        date_words[1] = date_words[1].replace(',','')
        strdate = ','.join(date_words)
        final_date = datetime.datetime.strptime(strdate, "%B,%d,%Y")
        date_list.append(final_date)
    return date_list[2:]

def get_rating(soup):
    data_str = ""
    rating_list = []
    for item in soup.find_all("a", class_="a-link-normal"):
        if item.get('title') and 'out' in item.get('title'):
            # if item.get('title')
            data_str = data_str + item.get('title')
            rating_list.append(int(data_str[:1]))
            data_str = ""
    return rating_list

def getdata(url):
    r = requests.get(url, headers=HEADERS)
    return r.text

def get_review_content(soup):
    data_str = ""
    for item in soup.find_all("span", class_="a-size-base review-text review-text-content"):
        data_str = data_str + item.get_text()
    intermediate = data_str.split("\n")
    result = [review for review in intermediate if review!='' ]
    return (result)

def html_code(url):

    htmldata = getdata(url)
    soup = BeautifulSoup(htmldata, 'html.parser')
    return (soup)


def main():
    timedelta = int(sys.argv[1])
    logger.info(f'Fetching reviews from the last {timedelta} days')
    q = sqs_helper.SQSQueue(queueName=AWS_SQS_QUEUE_NAME)   
    ASIN_list = [('B00NH11H38','Amazon Basics Digital Optical Audio Toslink Cable for Sound Bar, TV - 6 Feet (1.8 Meters)'),
        ('B07SHBQY7Z','Sleep Headphones Wireless, Perytong Bluetooth Sports Headband Headphones with Ultra-Thin HD Stereo Speakers Perfect for Sleeping,Workout,Jogging,Yoga,Insomnia, Air Travel, Meditation'),
        ('B01HTH3C8S','Anker Soundcore Mini, Super-Portable Bluetooth Speaker with 15-Hour Playtime, 66-Foot Bluetooth Range, Enhanced Bass, Noise-Cancelling Microphone - Black'),
        ('B07CRG94G3',' Seagate Portable 2TB External Hard Drive Portable HDD – USB 3.0 for PC, Mac, PS4, & Xbox - 1-Year Rescue Service (STGX2000400)'),
        ('B07QK2SPP7','JBL Flip 5 Portable Waterproof Wireless Bluetooth Speaker with up to 12 Hours of Battery Life - Black'),
        ('B07SJTHHRB','JBL GO2 Ultra Portable Waterproof Wireless Bluetooth Speaker with up to 5 Hours of Battery Life - Blue'),
        ('B016XTADG2','Bluetooth Speakers, Anker Soundcore Bluetooth Speaker Upgraded Version with Stereo Sound, BassUp Technology, 24H Playtime, Built-in Mic, Portable Wireless Speaker for iPhone, Samsung'),
        ('B0BCN4FXD5','Chromecast with Google TV (HD) - Streaming Stick Entertainment On Your TV with Voice Search - Watch Movies, Shows, and Live TV in 1080p HD - Snow'),
        ('B07N1WW638','TP-Link AC750 WiFi Extender (RE220) - Covers Up to 1,200 Sq.ft and 20 Devices, Up to 750Mbps, Dual Band WiFi Range Extender, WiFi Booster to Extend Range of WiFi Internet Connection'),
        ('B00A128S24','TP-Link TL-SG105 5 Port Gigabit Unmanaged Ethernet Network Switch, Ethernet Splitter, Plug and Play, Fanless Metal Design, Shielded Ports, Traffic Optimization, Limited Lifetime Protection')
        ]
    
    # ASIN_list = ['B07CRG94G3']
    try:
        for asin, name in ASIN_list:
            price, url = asin_to_price_and_reviews(asin)
            soup = html_code(url)
            dates = get_date(soup)
            required_timeline_review_dates = []
            week_ago_datetime = datetime.datetime.now() - datetime.timedelta(days=timedelta)
            for date in dates:
                if date > week_ago_datetime:
                    required_timeline_review_dates.append(date)
            reviews = get_review_content(soup)
            ratings = get_rating(soup)
            reviews_list = []
            try:
                for i in range(len(required_timeline_review_dates)):
                    reviews_list.append({'timestamp':str(required_timeline_review_dates[i]),'rating':ratings[i], 'review_content':reviews[i]})
                for review in reviews_list:
                    output_json = {'asin':asin, 'name':name, 'price':price, 'timestamp':review['timestamp'], 'rating':review['rating'], 'review_content':review['review_content'] }
                    logger.info(f'Sending review data : {output_json}')
                    response = q.send(Message=output_json)
                    logger.info(f'Review data sent to SQS, response : {response}')
            except Exception as e:
                logger.warn(f'Record not processed - {e}')
                continue
    except Exception as e:
        logger.error(f'Exception raised - {e}')
        exit(0)

if __name__ == '__main__':
    main()


    
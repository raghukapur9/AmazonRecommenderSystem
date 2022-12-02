import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

HEADERS = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/90.0.4430.212 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})
def getdata(url):
    try:
        r = requests.get(url, headers=HEADERS)
    except:
        logger.error('Failed to get data from URL')
    return r.text
  
def html_code(url):
  
    htmldata = getdata(url)
    soup = BeautifulSoup(htmldata, 'html.parser')
    return (soup)

def asin_to_price_and_reviews(asin):
    if asin is None or asin == '':
        raise Exception("ASIN cannot be empty")
    url = f'https://www.amazon.ca/s?k={asin}&s=review-rank' #sorted by reviews
    soup = html_code(url)
    data_str = ""
    for item in soup.find_all("a", class_="a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"):
        data_str = data_str + item['href']
    result = data_str.split("\n")
    product_page_url = f'https://www.amazon.ca{result[0]}' #URL of the first product
    logger.info(f'Product page URL - {product_page_url}')
    soup = html_code(product_page_url)
    data_str = ""
    for item in soup.find_all("a", class_="a-link-emphasis a-text-bold"):
        data_str = data_str + item['href']
    result = data_str.split("\n")
    price_list = []
    for item in soup.find_all("span", class_="a-price-whole"): #for the price
        price_list.append(item.text)
    price = int(price_list[0].replace('.',''))
    review_page_url = f'https://www.amazon.ca{result[0]}&sortBy=recent&pageNumber=1' #review page and reviews ordered by recent
    logger.info(f'Review page URL - {review_page_url}')
    return price, review_page_url

import os
import requests
from bs4 import BeautifulSoup
import re
import boto3
from datetime import datetime

# 텔레그램 봇 설정 (환경 변수에서 읽기)
# 한국 뉴스용
KOREAN_TOKEN = os.environ['TELEGRAM_KOREAN_TOKEN']
KOREAN_CHAT_ID = os.environ['TELEGRAM_KOREAN_CHAT_ID']
korean_bot_api_url = f'https://api.telegram.org/bot{KOREAN_TOKEN}/sendMessage'

# 미국 뉴스용
AMERICAN_TOKEN = os.environ['TELEGRAM_AMERICAN_TOKEN']
AMERICAN_CHAT_ID = os.environ['TELEGRAM_AMERICAN_CHAT_ID']
american_bot_api_url = f'https://api.telegram.org/bot{AMERICAN_TOKEN}/sendMessage'

# DynamoDB 클라이언트 생성
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('news_items')  # 기존 테이블 사용, 필요 시 분리 가능

def lambda_handler(event, context):
    print("Lambda 함수 시작")
    send_korean_news()
    send_american_news()
    print("Lambda 함수 종료")
    return {
        'statusCode': 200,
        'body': 'Success'
    }

# 한국 뉴스 관련 함수들
def fetch_korean_news(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    news_items = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        today = datetime.now().strftime("%Y%m%d")
        news_list = soup.find_all('dd', class_='articleSubject')

        for news in news_list:
            title_tag = news.find('a')
            if title_tag:
                title = title_tag.text.strip()
                link = 'https://finance.naver.com' + title_tag['href']
                # 링크에서 날짜 필터링
                match = re.search(r"date=(\d+)", link)
                if match and match.group(1) == today:
                    # DynamoDB에서 중복 메시지 확인
                    if not is_news_sent(title):
                        # 링크에서 article_id와 office_id 추출
                        match = re.search(r"article_id=(\d+)&office_id=(\d+)", link)
                        if match:
                            article_id = match.group(1)
                            office_id = match.group(2)
                            new_link = f"https://n.news.naver.com/mnews/article/{office_id}/{article_id}"
                            news_items.append((title, new_link))
                            # DynamoDB에 추가
                            save_news_item(title, new_link)
    else:
        print(f"Error fetching Korean news: {response.status_code}")
    return news_items

def send_korean_news():
    print("Sending Korean news")
    korean_urls = [
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=401',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=402',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=403',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=404',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=429'
    ]
    for url in korean_urls:
        news_items = fetch_korean_news(url)
        for title, link in news_items:
            message = f"{title}\n{link}"
            send_message(korean_bot_api_url, KOREAN_CHAT_ID, message)

# 미국 뉴스 관련 함수들
def fetch_american_news(url):
    response = requests.get(url)
    news_items = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        h3_tags = soup.find_all('h3', class_='Mb(5px)')

        for h3 in h3_tags:
            a_tag = h3.find('a')
            if a_tag:
                title = a_tag.text.strip()
                link = a_tag['href']
                full_link = f"https://finance.yahoo.com{link}" if link.startswith('/') else link
                # DynamoDB에서 중복 메시지 확인
                if not is_news_sent(title):
                    news_items.append((title, full_link))
                    # DynamoDB에 추가
                    save_news_item(title, full_link)
    else:
        print(f"Error fetching American news: {response.status_code}")
    return news_items

def send_american_news():
    print("Sending American news")
    american_urls = [
        "https://finance.yahoo.com/topic/latest-news/", 
        "https://finance.yahoo.com/topic/stock-market-news",
        "https://finance.yahoo.com/topic/earnings",
        "https://finance.yahoo.com/topic/yahoo-finance-originals",
        "https://finance.yahoo.com/topic/tech",
        "https://finance.yahoo.com/topic/crypto"
    ]
    for url in american_urls:
        news_items = fetch_american_news(url)
        for title, link in news_items:
            message = f"{title}\n{link}"
            send_message(american_bot_api_url, AMERICAN_CHAT_ID, message)

# 공통 함수들
def send_message(bot_api_url, chat_id, text):
    payload = {
        'chat_id': chat_id,
        'text': text
    }
    try:
        response = requests.post(bot_api_url, data=payload, timeout=10)
        response.raise_for_status()
        print(f"Message sent to chat_id {chat_id}: {text}")
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 1))
            print(f"Rate limited by Telegram. Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
            try:
                response = requests.post(bot_api_url, data=payload, timeout=10)
                response.raise_for_status()
                print(f"Message sent after retry to chat_id {chat_id}: {text}")
            except Exception as e:
                print(f"Failed to send message after retry: {e}")
        else:
            print(f"Error sending message to chat_id {chat_id}: {e}")
    except Exception as e:
        print(f"Unexpected error sending message to chat_id {chat_id}: {e}")

def is_news_sent(title):
    print(f"Checking if news is sent: {title}")
    try:
        response = table.get_item(Key={'title': title})
        return 'Item' in response
    except Exception as e:
        print(f"Error checking DynamoDB: {e}")
        return False

def save_news_item(title, link):
    print(f"Saving news item: {title}")
    try:
        today = datetime.now().strftime("%Y%m%d")
        table.put_item(Item={'title': title, 'link': link, 'date_sent': today})
    except Exception as e:
        print(f"Error saving to DynamoDB: {e}")

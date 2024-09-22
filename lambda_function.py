#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 17 23:36:50 2024

@author: sangwonchae
This is to share Yahoo News via Telegram
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import requests
from bs4 import BeautifulSoup
from telegram import Bot, error as telegram_error
import re
import sqlite3
from datetime import datetime

# 텔레그램 봇 설정
#TOKEN = '6821721487:AAHsHBfSUF5TbwsvcyF4sbQ9C7Jum1P8FW0'
#CHAT_ID = '-1001928078035'
TOKEN = '6996569159:AAE8L14LimEkhj8q73zM1ZuVIZvh0JfY8Ng'
CHAT_ID = '-1002025567712'
bot = Bot(token=TOKEN)

# 데이터베이스 연결 및 테이블 생성
conn = sqlite3.connect('ynews_sent.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS news_items (
        title TEXT PRIMARY KEY,
        link TEXT,
        date_sent TEXT
    )
''')
conn.commit()

async def fetch_korean_news(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
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
                    # 중복 메시지 확인
                    cursor.execute("SELECT * FROM news_items WHERE title=?", (title,))
                    if not cursor.fetchone():
                        # 링크에서 article_id와 office_id 추출
                        match = re.search(r"article_id=(\d+)&office_id=(\d+)", link)
                        if match:
                            article_id = match.group(1)
                            office_id = match.group(2)
                            new_link = f"https://n.news.naver.com/mnews/article/{office_id}/{article_id}"
                            news_items.append(f"{title}\n{new_link}")
                            # DB에 추가
                            cursor.execute("INSERT INTO news_items (title, link, date_sent) VALUES (?, ?, ?)", (title, new_link, today))
                            conn.commit()
    return news_items

async def fetch_american_news(url):
    response = requests.get(url)
    news_items = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        h3_tags = soup.find_all('h3', class_='Mb(5px)')

        for h3 in h3_tags:
            a_tag = h3.find('a', class_='Fw(b) Fz(18px) Lh(23px) LineClamp(2,46px) Fz(17px)--sm1024 Lh(19px)--sm1024 LineClamp(2,38px)--sm1024 mega-item-header-link Td(n) C(#0078ff):h C(#000) LineClamp(2,46px) LineClamp(2,38px)--sm1024 not-isInStreamVideoEnabled')
            if a_tag:
                title = a_tag.text.strip()
                link = a_tag['href']
                full_link = f"https://finance.yahoo.com{link}" if link.startswith('/') else link
                # 중복 메시지 확인
                cursor.execute("SELECT * FROM news_items WHERE title=?", (title,))
                if not cursor.fetchone():
                    news_items.append(f"{title}\n{full_link}")
                    # DB에 추가
                    today = datetime.now().strftime("%Y%m%d")
                    cursor.execute("INSERT INTO news_items (title, link, date_sent) VALUES (?, ?, ?)", (title, full_link, today))
                    conn.commit()
    return news_items

async def send_news():
    korean_urls = [
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=401',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=402',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=403',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=404',
        'https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=429'
    ]
    
    american_urls = [
        "https://finance.yahoo.com/topic/latest-news/", 
        "https://finance.yahoo.com/topic/stock-market-news",
        "https://finance.yahoo.com/topic/earnings",
        "https://finance.yahoo.com/topic/yahoo-finance-originals",
        "https://finance.yahoo.com/topic/tech",
        "https://finance.yahoo.com/topic/crypto"
    ]
    
    # Fetch and send Korean news
    '''for url in korean_urls:
        news_items = await fetch_korean_news(url)
        for item in news_items:
            try:
                await bot.send_message(chat_id=CHAT_ID, text=item)
                print(item)
                await asyncio.sleep(1)  # 각 메시지 전송 사이에 딜레이 추가
            except Exception as e:
                print(f"Error sending message: {e}")
                if isinstance(e, telegram_error.RetryAfter):
                    retry_delay = e.retry_after + 1
                    print(f"Retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    await bot.send_message(chat_id=CHAT_ID, text=item)
    '''                
    # Fetch and send American news
    for url in american_urls:
        news_items = await fetch_american_news(url)
        for item in news_items:
            try:
                await bot.send_message(chat_id=CHAT_ID, text=item)
                print(item)
                await asyncio.sleep(3)  # 각 메시지 전송 사이에 딜레이 추가
            except Exception as e:
                print(f"Error sending message: {e}")
                if isinstance(e, telegram_error.RetryAfter):
                    retry_delay = e.retry_after + 1
                    print(f"Retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    await bot.send_message(chat_id=CHAT_ID, text=item)

async def run_scheduler():
    while True:
        await send_news()
        await asyncio.sleep(1800)  # 30분마다 반복

if __name__ == '__main__':
    asyncio.run(run_scheduler())

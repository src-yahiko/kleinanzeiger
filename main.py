from time import sleep
import requests
import json
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from datetime import datetime, timedelta
import sqlite3

class Kleinanzeige:
    def __init__(self, scrape_time, article_id, title, date, image, district, zipcode, price, price_fixed, views):
        self.scrape_time = scrape_time
        self.article_id = article_id
        self.title = title
        self.published = date
        self.image_url = image
        self.district = district
        self.zipcode = zipcode
        self.price = price
        self.price_fixed = price_fixed
        self.views = views


def scrape(link, items):
    sleep(1)
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    with requests.Session() as c:
        soup = BeautifulSoup(webpage, 'html5lib')
        main_section = soup.find('ul', attrs={'id': 'srchrslt-adtable'})
        ul = main_section.findAll('li', attrs={'class': 'ad-listitem lazyload-item'})
        for li in ul:
            image = li.find('div', attrs={'class': 'imagebox srpimagebox'})['data-imgsrc']
            title = li.find('div', attrs={'class': 'aditem-main--middle'}).find('a').get_text()
            city = ' '.join(li.find('div', attrs={'class': 'aditem-main--top'}).get_text().split('\n'))\
                .strip().replace('  ', '')
            zipcode = city.split(' ')[0]
            date = ""
            if city.find(',') >= 0:
                date = city.split(',')[0].split(' ')[-1] \
                    .replace('Heute', datetime.today().strftime("%Y-%m-%d"))) \
                    .replace('Gestern', (datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d"))
            else:
                date = datetime.strptime((city.split(' ')[-1]), '%d.%m.%Y').strftime("%Y-%m-%d")
            district = ' '.join(city.split(',')[0].split(' ')[1:-1])
            price_tag = li.find('p', attrs={'class': 'aditem-main--middle--price'}).get_text().strip()\
                .replace(',', '.')
            vb = True if price_tag.find("VB") >= 0 else False
            price = [float(s) for s in price_tag.split(" ") if s.isdigit()]
            price = price[0] if len(price) == 1 else None
            link = li.find('article')['data-href']
            views_link = 'https://www.ebay-kleinanzeigen.de/s-vac-inc-get.json?adId=' + link.split('/')[-1].split('-')[0]
            sleep(2.2)
            views = json.loads(urlopen(Request(views_link, headers={'User-Agent': 'Mozilla/5.0'})).read())['numVisits']
            items.append(Kleinanzeige(
                scrape_time=datetime.today().strftime("%Y-%m-%d-%H-%m"),
                title=title,
                date=date
                image=image,
                district=district,
                zipcode=zipcode,
                price=price,
                price_fixed=not vb,
                views=views,
                article_id=link.split('/')[-1].split('-')[0]
            ).__dict__)
            print(len(items), title)
            #return items

        next_page = soup.find('a', attrs={'class': 'pagination-next'})
        if next_page is not None:
            return items + scrape('https://www.ebay-kleinanzeigen.de/' + next_page['href'], items)
        return items

if __name__ == "__main__":
    ebay_link = 'https://www.ebay-kleinanzeigen.de/s-multimedia-elektronik/berlin/anbieter:privat/anzeige:angebote/preis:50:/gameboy-advance/k0c161l3331'
    traffic = scrape(ebay_link, [])
#    print(json.dumps(traffic, indent=4, ensure_ascii=False))
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(traffic, f, ensure_ascii=False, indent=4)
    connection = sqlite3.connect('db.sqlite')
    cursor = connection.cursor()
    cursor.execute('Create Table if not exists Kleinanzeigen (scrape_time, title, published, image_url, district, zipcode, price, price_fixed, views, article_id)')

    traffic = json.load(open('data.json'))
    columns = ['scrape_time', 'title', 'published', 'image_url', 'district', 'zipcode', 'price', 'price_fixed', 'views', 'article_id']
    for row in traffic:
 #       print(row)
        keys = tuple(row[c] for c in columns)
        cursor.execute('insert into Kleinanzeigen values(?,?,?,?,?,?,?,?,?,?)', keys)

    connection.commit()
    connection.close()

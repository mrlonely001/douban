import scrapy
from scrapy.spider import Spider
import random
import time
import pymongo
from douban.items import MovieCommentItem

class Got7Crawler(Spider):
    name = "got7_crawler"
    allowed_domains = ["douban.com"]


    header = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding":"gzip, deflate, sdch",
        "Connection":"keep-alive",
        "Cache-Control":"max-age=0",
        "Host":"movie.douban.com",
        "Referer":"https://movie.douban.com/subject/26607693/comments",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }


    def getproxyfrommongo(self):
        client = pymongo.MongoClient(host="127.0.0.1", port=27017)
        db = client['proxy']
        coll = db['FreeProxyItem']
        proxyArr = []
        for item in coll.find({"crawl_time":{"$gt":"2017-09-03 00:00:00"}}).sort("crawl_time", pymongo.DESCENDING):
            proxy = {}
            proxy['ip'] = item["ip"]
            proxy['port'] = item["port"]
            proxyArr.append(proxy)
        return proxyArr

    def start_requests(self):
        proxyArr = self.getproxyfrommongo()
        print ("++++++++++"+str(len(proxyArr))+" proxy to use+++++++++++")
        root_url = "https://movie.douban.com/subject/26607693/comments?start=<pageth>&limit=20&sort=new_score&status=P"
        starturls = []
        for i in range(0,500):
            starturls.append(root_url.replace("<pageth>",str(i*20+i)))
        for i in range(len(starturls)):
            if i==0:
                continue
            time.sleep(random.randint(1,5))
            proxy = random.choice(proxyArr)
            proxyStr = "http://" + proxy['ip'] + ":" + proxy['port']
            self.header['Referer'] = starturls[i-1]
            yield scrapy.Request(url=starturls[i], callback=self.parse, headers=self.header,meta={'proxy':proxyStr})

    def parse(self, response):
        divs = response.css("div.comment-item")
        for div in divs:
            usr = div.css("span.comment-info a")
            usrname = usr[0].css("::text").extract_first()
            usraddr = usr[0].css("::attr(href)").extract_first()
            commentinfo = div.css("span.comment-info span")
            starnum = 0
            if len(commentinfo) == 3:
                starstr = commentinfo[1].css("::attr(class)").extract_first()
                starnum = starstr.replace("allstar","").replace(" rating","")
            commenttime = commentinfo[len(commentinfo)-1].css("::attr(title)").extract_first()
            voteinfo = div.css("span.comment-vote span")
            votenum = voteinfo[0].css("::text").extract_first()
            conmment = div.css("p")[0].css("::text").extract_first()
            print (usrname + usraddr + starnum + commenttime + votenum + conmment)
            item = MovieCommentItem()
            item['_id'] = usraddr
            item['usrname'] = usrname
            item['usraddr'] = usraddr
            item['starnum'] = starnum
            item['commenttime'] = commenttime
            item['votenum'] = votenum
            item['conmment'] = conmment
            yield item

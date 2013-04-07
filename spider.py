# -*- coding: UTF-8 -*
'''
Created on 2013-4-5
爬虫
@author: RobinTang
'''

import dbm
from urllib import FancyURLopener
import re

TYPERECORDMATCHHTML = 0
TYPERECORDALLHTML = 1
TYPENORECORDHTML = 2

class SinOpener(FancyURLopener):
    # 下面是爬虫的标识，现在用的是Google的爬虫标识，有的网站不让随随便便爬，只能盗用Google的标识了，研究而已，不要乱爬
    version = 'Googlebot'

sinopener = SinOpener()


def getptnlev(relurl):
    '''
    获取相对地址层数
    '''
    if relurl[0]=='.' and relurl[1]=='.' and relurl[2]=='/':
        return getptnlev(relurl[3:]) + 1
    else:
        return 0


def getptndir(baseurl, lev):
    '''
    获取父文件夹
    '''
#    print ' pt:%s'%baseurl
    ix = baseurl.rindex('/')
    if lev:
        return getptndir(baseurl[0:ix], lev-1)
    else:
        return baseurl[0:ix+1]
def getabsurl(baseurl, relurl):
    '''
    获取绝对地址
    '''
    lev = getptnlev(relurl)
#    print 'bas:%s   rel:%s'%(baseurl, relurl)
    purl = getptndir(baseurl, lev)
    if relurl.rfind('./')>=0:
        surl = relurl[relurl.rfind('./')+1:]
    else:
        surl = relurl
    if purl[-1]=='/' and surl[0] == '/':
        surl = surl[1:]
#    print 'purl:%s   surl:%s'%(purl, surl)
    return '%s%s'%(purl, surl)

def gethtml(url):
    '''
    获取HTML
    '''
    print 'get:%s'%url
    html = None
    status = False
    try:
        doc = sinopener.open(url)
        if doc:
#            print doc.info()
            contype = doc.info().getheader('Content-Type').lower()
            url = doc.geturl().replace('/./', '/')
            charset = None
            if contype.find('html')>=0:
                html = doc.read()
                chs = re.findall('charset\s*=\s*([^\s,^;]*)', contype)
                if chs and len(chs)>0:
                    charset = chs[0]
                else:
                    chs = re.findall('charset\s*=\s*([^\s,^;,^"]*)', html)
                    if chs and len(chs)>0:
                        charset = chs[0]
                if charset:
                    charset = charset.lower()
                    try:
                        html = html.decode(charset)
                    except:
                        try:
                            html = html.decode('utf-8')
                        except:
                            try:
                                html = html.decode('gbk')
                            except:
                                html = html
                status = True
            else:
                html = 'not html document'
#            print 'ctp:%s'%contype
#            print 'cset:%s'%charset
    except Exception, e:
        html = '%s'%e
    return {'status':status, 'url':url, 'html':html}

def geturls(html, baseurl=None):
    '''
    获取页面中的链接
    '''
#    print 'base:%s'%baseurl
    p = '<a[^>]*href\s*=\s*"([^", ^#]*)[^>]*"'
    allurl = re.findall(p, html, re.IGNORECASE | re.MULTILINE)
    if baseurl:
        p = re.compile('((https|http)://[^/]*)', re.IGNORECASE)
        host = p.search(baseurl).group(0)
        for i in range(len(allurl)):
            url = allurl[i]
            if url and len(url) and not p.match(url):
                if url[0]=='/':
                    allurl[i] = ('%s%s'%(host, url))
                else:
                    allurl[i] = getabsurl(baseurl, url)
        pass
    return allurl

def getcontex(html, keyword):
    '''
    获取相关上下文
    '''
    p = '([^>,^"]*%s[^<,^"]*)'%unicode(keyword, 'utf-8')
#    print 'p:%s'%p
#    print 'html:%s'%html
    cxts = re.findall(p, html, re.IGNORECASE | re.MULTILINE)
    if cxts and len(cxts):
        for i in range(len(cxts)):
            cxts[i] = cxts[i].encode('UTF-8')
    return cxts

def gettitle(html):
    '''
    获取文档标题
    '''
    p = '<title>([^<]*)</title>'
    s = re.search(p, html, re.IGNORECASE | re.MULTILINE)
    if s and len(s.groups()):
        return s.group(1).strip()
    else:
        return None

def dooneurl(taskid=0):
    '''
    执行一次爬行
    '''
    rec = dbm.getoneurl(taskid)
    if rec:
        rec['status'] = 1 # 设置为在爬
        rec['fetchtime'] = dbm.timestring()
        dbm.setoneurl(**rec)
        dbm.commit()
        res = gethtml(rec['url'])
        html = res['html']
        stat = res['status']
        baseurl = rec['baseurl']
        if rec['url'] != res['url']:
            baseurl = dbm.getbaseurl(res['url'])
        if stat:
            ctxs = getcontex(html, rec['keyword'])
            titl = gettitle(html)
#            print 'title:%s'%titl
#            print 'html:%s'%html
            rec['title'] = titl
            if rec['type'] == TYPERECORDALLHTML:
                rec['html'] = html
#            print "ctxs:%s"%ctxs
            count = 0
            if ctxs and len(ctxs):
                count = len(ctxs)
                rec['count'] = count
                rec['context'] = ';;;'.join(ctxs)
                if rec['type'] == TYPERECORDMATCHHTML:
                    rec['html'] = html
                
            # 继续搜索
            if rec['deep'] < rec['maxdeep']:
                urls = geturls(html, baseurl)
#                print "urls:%s"%urls
                if urls and len(urls):
                    rec['childcount'] = len(urls)
                    for i in range(len(urls)):
                        url = urls[i]
                        if rec['urlflag'] and url.find(rec['urlflag'])==-1:
                            continue
                        one = dbm.getoneurl(taskid, url)
                        if not one:
                            print 'add url: %s'%url
                            dbm.addoneurl(taskid=taskid, 
                                            pid=rec['id'], 
                                            url=url, 
                                            keyword=rec['keyword'],
                                            type=rec['type'], 
                                            deep=rec['deep']+1,
                                            urlflag=rec['urlflag'],
                                            power=count,
                                            maxdeep=rec['maxdeep'])
            
            rec['status'] = 2 # 设置为已爬
            rec['completetime'] = dbm.timestring()
            dbm.setoneurl(**rec)
            dbm.commit()
        else:
            print 'get url fail:%s'%rec['url']
            print 'fail info:%s'%html
            rec['html'] = html
            rec['status'] = 3 # 设置为出错
            rec['completetime'] = dbm.timestring()
            dbm.setoneurl(**rec)
            dbm.commit()
        return True
    else:
        print 'task%d is success'%taskid
        return False

def dotask(taskid):
    '''
    执行一次爬行任务
    '''
    res = True
    try:
        res = dooneurl(taskid)
    except:
        pass
    return res

if __name__ == '__main__':
    taskid = 1
    dbm.clear()
    dbm.addoneurl(taskid=taskid, url='http://news.baidu.com/', urlflag='news.baidu.com', keyword='H7N9', maxdeep=2, type=TYPERECORDMATCHHTML)
    dbm.commit()
    while dotask(taskid):
        pass
    print 'fetch ok'

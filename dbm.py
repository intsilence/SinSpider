# -*- coding: UTF-8 -*
'''
Created on 2013-4-5
爬虫的数据库访问封装
@author: RobinTang
'''
import MySQLdb
import types
import time
urlstab = 'sinspider'

try:
#    尝试对SAE的数据库进行连接
    import sae
    conn=MySQLdb.connect(host=sae.const.MYSQL_HOST,user=sae.const.MYSQL_USER,passwd=sae.const.MYSQL_PASS,db=sae.const.MYSQL_DB,port=sae.const.MYSQL_PORT)
except:
#    连接失败，那么就认为是在本地环境
    conn=MySQLdb.connect(host='127.0.0.1',user='trb',passwd='123',db='dbp',port=3306)
cur=conn.cursor()
conn.set_character_set('utf8')


createtab = '''
CREATE TABLE IF NOT EXISTS `%s`(
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '父ID',
  `taskid` int(11) NOT NULL DEFAULT '0' COMMENT '任务ID，标识不同的任务关键字',
  `url` varchar(1024) NOT NULL COMMENT 'url',
  `urlflag` varchar(1024) DEFAULT NULL,
  `baseurl` varchar(1024) NOT NULL,
  `keyword` varchar(512) NOT NULL COMMENT '关键字',
  `power` int(11) DEFAULT '0' COMMENT '权值',
  `deep` int(11) NOT NULL DEFAULT '0' COMMENT '深度',
  `maxdeep` int(11) NOT NULL DEFAULT '10' COMMENT '最大深度',
  `status` int(11) NOT NULL DEFAULT '0' COMMENT '状态,0:未爬 1:在爬 2:已爬',
  `type` int(11) NOT NULL DEFAULT '0' COMMENT '爬虫规则，应该遗传给子URL',
  `count` int(11) NOT NULL DEFAULT '0' COMMENT '匹配个数',
  `childcount` int(11) DEFAULT NULL,
  `context` longtext,
  `title` varchar(512) DEFAULT NULL,
  `html` longtext COMMENT 'HTML',
  `createtime` timestamp NULL DEFAULT NULL COMMENT '创建时间',
  `fetchtime` timestamp NULL DEFAULT NULL,
  `completetime` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ID索引` (`id`),
  KEY `URL索引` (`url`(255)),
  KEY `状态索引` (`status`),
  KEY `权重索引` (`power`),
  KEY `TASKID索引` (`taskid`),
  KEY `匹配数目索引` (`count`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''%urlstab

def getkeysvals(**args):
    '''
    键值对重组
    '''
    keys = ''
    vals = ''
    stas = ''
    conn = cur._get_db()
    for (k,v) in  args.items():
        if type(v) is types.NoneType:
            continue
        if len(keys):
            keys += ', '
            vals += ', '
            stas += ', '
        v = conn.literal(v)
        keys += '`%s`'%k
        vals += '%s'%v
        stas += '`%s`=%s'%(k, v)
    return {'keys':keys, 'vals':vals, 'stas':stas}


def query(sql):
    '''
    执行查询
    '''
#    print sql
    return cur.execute(sql)

def commit():
    '''
    执行确认
    '''
    conn.commit()

def timestamp():
    '''
    时间戳
    '''
    return int(time.time())

def timestring():
    '''
    时间字符串
    '''
    return time.strftime('%Y-%m-%d %H:%M:%S')

def getbaseurl(url):
    '''
    获取基地址
    '''
    ix = url.rindex('/')
    if url[ix-1] == '/':
        if url[-1] == '/':
            baseurl = url
        else:
            baseurl = url + '/'
    else:
        baseurl = url[0:url.rindex('/')+1]
    return baseurl

def addoneurl(**args):
    '''
    添加爬行任务
    '''
    if not args.has_key('baseurl'):
        url = args['url']
        args['baseurl'] = getbaseurl(url)
    if not args.has_key('createtime'):
        args['createtime'] = timestring()
    kvs = getkeysvals(**args)
    sql = 'insert into `%s`(%s) values(%s)'%(urlstab, kvs['keys'], kvs['vals'])
    return query(sql)

def setoneurl(**args):
    '''
    更新爬行任务
    '''
    if args.has_key('id'):
        kvs = getkeysvals(**args)
        sql = 'update `%s` set %s where id=%d'%(urlstab, kvs['stas'], args['id'])
        return query(sql)
    else:
        raise ValueError, 'Must have id'
    
def getoneurl(taskid=0, url=None):
    '''
    获取一个爬行任务
    '''
    if taskid:
        if url:
            count=query('select * from `%s` where url="%s" and taskid=%d order by `power` desc limit 1'%(urlstab, url, taskid))
        else:
            count=query('select * from `%s` where status=0 and taskid=%d order by `power` desc limit 1'%(urlstab, taskid))
    else:
        count=query('select * from `%s` where status=0 order by `power` desc limit 1'%urlstab)
    if count:
        names = [x[0] for x in cur.description]
        row = cur.fetchone()
        return dict(zip(names, row))
    else:
        return None

def getcount(taskid, status):
    '''
    查询任务
    '''
    count=query('select count(*) as `count` from `%s` where taskid=%d and status=%d'%(urlstab, taskid, status))
    if count:
        row = cur.fetchone()
        return row[0]
    else:
        return 0

def getinfo(taskid):
    '''
    获取爬行状态
    '''
    count0 = getcount(taskid, 0)
    count1 = getcount(taskid, 1)
    count2 = getcount(taskid, 2)
    count3 = getcount(taskid, 3)
    return {'all':count0+count1+count2+count3, 'wait':count0, 'fetching':count1, 'success':count2, 'fail':count3}

def clear(taskid=0):
    '''
    清空
    '''
    if not taskid:
        sql = 'TRUNCATE TABLE  `%s`'%urlstab
    else:
        sql = 'delete from `%s` where `taskid`=%d'%(urlstab, taskid)

    return query(sql)


def close():
    '''
    关闭
    '''
    cur.close()
    conn.close()
    del cur
    del conn
def init():
    '''
    初始化
    '''
    try:
        query(createtab)
        commit()
    except:
        pass

if __name__ == '__main__':
    print getinfo(1)
    print 'sindb is ok!'





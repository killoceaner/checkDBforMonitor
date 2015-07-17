#!/usr/bin/env python
# -* - coding: UTF-8 -* -
__author__ = 'houxiang'

import EmailHandler
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import  time
import  MySQLdb
import  logging
try:
    import cStringIO as StringIO
except ImportError:
    import  StringIO

#current time
cur_time = time.strftime("%Y/%m/%d/%H"+":"+"%M")

#for run
#emailList = {"Mr Yin":"jack_nudt@163.com","Mr WangTao":"taowang.2005@outlook.com","gyiang":"getbox@126.com","zf":""}
emailList = {"Mr Yin":"450717607@qq.com","Mr WangTao":"450717607@qq.com","gyiang":"450717607@qq.com","zf":"450717607@qq.com"}
#for log
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('log.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
dicTable={'openhub_project':'openhub_html_detail','csdn_ask':"csdn_ask_html_detail",'csdn_blogs':'csdn_blog_html_detail','csdn_topics':'csdn_topic_html_detail','cnblog_news':'cnblogs_news_html_detail','cnblog_question':'cnblogs_q_solved_html_detail','dewen_question':'dewen_question_html_detail',
            'freecode_projects':'freecode_html_detail','iteye_ask':'iteye_ask_html_detail','oschina_project':'oschina_project_html_detail','oschina_question':'oschina_question_html_detail','sourceforge_project':'sourceforge_html_detail','stackoverflow_q':'stackoverflow_html_detail',
            '51cto_blog':'51cto_blog_html_detail','codeproject':'codeproject_html_detail','lupaworld':'lupaworld_html_detail','iteye_blog':'iteye_blog_html_detail','gna':'gna_html_detail','apache':'apache_html_detail','phpchina':'phpchina_html_detail','softpedia':'softpedia_html_detail','linuxtone':'linuxtone_html_detail','slashdot':'slashdot_html_detail',
            'neitui':'neitui_html_detail','lagou':'lagou_html_detail'}

websiteSum={'openhub_project':0,'csdn_ask':0,'csdn_blogs':0,'csdn_topics':0,'cnblog_news':0,'cnblog_question':0,'dewen_question':0,
            'freecode_projects':0,'iteye_ask':0,'oschina_project':0,'oschina_question':0,'sourceforge_project':0,'stackoverflow_q':0,
            '51cto_blog':0,'codeproject':0,'lupaworld':0,'iteye_blog':0,'gna':0,'apache':0,'phpchina':0,'softpedia':0,'linuxtone':0,'slashdot':0,
            'neitui':0,'lagou':0}

websiteInfo = {'openhub_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'csdn_ask':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'csdn_blogs':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'csdn_topics':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'cnblog_news':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'cnblog_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'dewen_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0}, 'freecode_projects':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'iteye_ask':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'oschina_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'oschina_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'sourceforge_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'stackoverflow_q':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'51cto_blog':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'codeproject':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'lupaworld':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'iteye_blog':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'gna':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'apache':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'phpchina':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'softpedia':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},
               'linuxtone':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'slashdot':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'neitui':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0},'lagou':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0}}

#for test
#print websiteInfo['openhub_project']['extract_rate']
#print websiteInfo['openhub_project']['flow_num']

#for run
"""
sourceDB={'host':"192.168.80.104","user":"influx","passwd":"influx1234","port":3306,"extractDB":"extract_result","crawlerDB":"pages"}
targetDB={'host':"192.168.80.130","user":"trustie","passwd":"1234","port":3306,"database":"ossean_production"}
"""
#for test

sourceDB={"host":'localhost',"user":'root',"passwd":'root',"port":3306,"extractDB":'test_db',"crawlerDB":"pages"}
targetDB={"host":'localhost',"user":'root',"passwd":'root',"port":3306,"database":'test_db'}

SourceConn = MySQLdb.connect(host=sourceDB["host"],user=sourceDB["user"],passwd=sourceDB["passwd"],port=sourceDB["port"])
TargetConn = MySQLdb.connect(host=targetDB["host"],user=targetDB["user"],passwd=targetDB["passwd"],port=sourceDB["port"])
'''
SourceConn = MySQLdb.connect(host='localhost',user='root',passwd='root',port=3306)
TargetConn = MySQLdb.connect(host='localhost',user='root',passwd='root',port=3306)
'''

#sql
querySqlOfTflow = "SELECT MAX(EndID)-MIN(BeginID),SUM(flow) FROM `migrationTask` WHERE SourceTableName = %s AND " \
                  "DATE_FORMAT(EndTime,'%%y-%%m-%%d') = DATE_FORMAT(NOW(),'%%y-%%m-%%d')"

updateSql = "UPDATE ossean_monitors SET flow_num =%s , flow_rate = %s ,extract_rate = %s,crawler_sum = %s WHERE website=%s"

#querySqlOfExtract = "SELECT day_crawler , day_extractor FROM `ossean_monitors` WHERE website = %s"

querSqlOfCrawlerSum = "SELECT COUNT(DISTINCT url) FROM "

querySqlOfExtractSUM = "SELECT total_num  FROM `ossean_monitors` WHERE website = %s"

querySqlOfWeekCrawler = "SELECT week_crawler FROM `ossean_monitors` WHERE website = %s"

def checkSourceDB(sql ,value='' ,flag=''):
    cur = SourceConn.cursor()
    #choose which database
    if flag == 'extract':
        SourceConn.select_db(sourceDB["extractDB"])
        count = cur.execute(sql,value)
    elif flag =='crawler':
        SourceConn.select_db(sourceDB["crawlerDB"])
        sql = sql+"`"+value+"`"
        count = cur.execute(sql)

   #count = cur.executemany(sql,value)
    print count
    #logger.info(count)
    result = cur.fetchone()
    SourceConn.commit()
    return result

def checktargetDB(sql ,value ):
    cur = SourceConn.cursor()
    SourceConn.select_db(targetDB["database"])
    count = cur.execute(sql,value)
    #count = cur.executemany(sql,value)
    print count
    #logger.info(count)
    result = cur.fetchone()
    SourceConn.commit()
    return result

def updateTargetDB(sql,*value):
    cur = TargetConn.cursor()
    TargetConn.select_db(targetDB["database"])
    count = cur.execute(sql,value)
    TargetConn.commit()
    return count
"""
def email(content , desAddr):
    sender = 'nudt_houxiang@163.com'
    receiver = desAddr
    subject = 'ossean data flow problems by monitors'
    smtpserver = 'smtp.163.com'
    username = 'nudt_houxiang@163.com'
    password = 'abc123456'
    msg = MIMEText(content,'plain','utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    smtp = smtplib.SMTP()
    smtp.connect('smtp.163.com')
    smtp.login(username, password)
    smtp.sendmail(sender, receiver, msg.as_string())
    smtp.quit()
"""
def closeCon():
    SourceConn.close()
    TargetConn.close()

def main():
    for  name in websiteInfo.keys():
        #***********************************calculate flow_num and flow_rate
        ans = list(checkSourceDB(querySqlOfTflow , name , "extract"))
        if ans[0] ==  None:
            ans[0] = 0
        if ans[1] == None:
            ans[1] = 0
        #calculate flow_rate
        if ans[1] == 0:
            pass
        else:
            websiteInfo[name]['flow_rate'] = ans[1]/ans[0]
        websiteInfo[name]['flow_num'] = ans[1]
        print(ans)
        #*************************************calculate extract_rate
        print dicTable[name]
        crawlerSum = list(checkSourceDB(querSqlOfCrawlerSum , dicTable[name], "crawler"))
        extractSum = list(checktargetDB(querySqlOfExtractSUM, name ))
        weekcrawler = list(checktargetDB(querySqlOfWeekCrawler , name))
        websiteSum[name] = extractSum
        if crawlerSum[0] == None:
            crawlerSum[0] = 0
        elif extractSum[0] == None:
            extractSum[0] = 0
        elif weekcrawler == None:
            weekcrawler[0] = 0

        if crawlerSum[0]==0 or extractSum[0] == 0:
            pass
        else:
            websiteInfo[name]['extract_rate'] = extractSum[0] / crawlerSum[0]
            websiteInfo[name]['crawler_sum'] = crawlerSum[0]
        websiteInfo[name]['weekcrawler'] = weekcrawler[0]
    for name , information in websiteInfo.items():
        flow_num = information['flow_num']
        flow_rate = information['flow_rate']
        extract_rate =  information['extract_rate']
        crawlerSum = information['crawler_sum']
       #condition_values = (flow_num ,flow_rate ,extract_rate , name)
        logger.info(name +":"+ "flow_num"+":"+str(flow_num) +" "+ "flow_rate"+":" + str(flow_rate)+" "+"extract_rate"+":"+str(extract_rate) )
        updateTargetDB(updateSql,flow_num,flow_rate,extract_rate,crawlerSum,name)
        if flow_num == 0 or flow_rate <= 0.7:
            content ="the site"+":"+name+" tflow is getting wrong,the data flow is broken , the charge of tflow is houxiang , please fix it ......."
            emailHandler = EmailHandler.Email(content,cur_time)
            emailHandler.set_desAddr("450717607@qq.com")
            emailHandler.send_email()
        elif extract_rate <= 0.7:
            content = "the website"+":"+name+" extract is geting wrong ,the extract rate is too low , the charge of extract is zhangfang , please fix it ...... "
            emailHandler = EmailHandler.Email(content,cur_time)
            emailHandler.set_desAddr("450717607@qq.com")
            emailHandler.send_email()
        elif crawlerSum == 0 :
            content = "the webiste"+":"+name+" crawler is getting wrong ,sum of crawler for this week is zero ,the charge of craweler is gyiang and lizhixing , please fix it ......."
            emailHandler = EmailHandler.Email(content,cur_time)
            emailHandler.set_desAddr("450717607@qq.com")
            emailHandler.send_email()
    closeCon()
    print  websiteInfo

def timer(n):
    while True:
        localTime = time.localtime()
        logger.info(localTime)
        main()
        time.sleep(n)

if __name__ == '__main__':
    timer(60)
    #timer(12*60*60)

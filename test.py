__author__ = 'houxiang'

import smtplib
from email.mime.text import MIMEText
from email.header import Header
import  time
import EmailHandler
import time
'''
sender = 'nudt_houxiang@163.com'
receiver = '450717607@qq.com'
subject = 'python email test'
smtpserver = 'smtp.163.com'
username = 'nudt_houxiang@163.com'
password = 'abc123456'

msg = MIMEText('hello','plain','utf-8')
msg['Subject'] = Header(subject, 'utf-8')

smtp = smtplib.SMTP()
smtp.connect('smtp.163.com')
smtp.login(username, password)
smtp.sendmail(sender, receiver, msg.as_string())
smtp.quit()

websiteInfo = {'openhub_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'csdn_ask':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'csdn_blogs':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'csdn_topics':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'cnblog_news':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'cnblog_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'dewen_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0}, 'freecode_projects':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'iteye_ask':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'oschina_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'oschina_question':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'sourceforge_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'stackoverflow_q':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'51cto_blog':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'codeproject':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'lupaworld':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'iteye_blog':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'gna':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'apache':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'phpchina':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'softpedia':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},
               'linuxtone':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'slashdot':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'neitui':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0},'lagou':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0}}

dicTable={'openhub_project':'openhub_html_detail','csdn_ask':'csdn_ask_html_detail','csdn_blogs':'csdn_blog_html_detail','csdn_topics':'csdn_topic_html_detail','cnblog_news':'cnblogs_news_html_detail','cnblog_question':'cnblogs_q_solved_html_detail','dewen_question':'dewen_question_html_detail',
            'freecode_projects':'freecode_html_detail','iteye_ask':'iteye_ask_html_detail','oschina_project':'oschina_project_html_detail','oschina_question':'oschina_question_html_detail','sourceforge_project':'sourceforge_html_detail','stackoverflow_q':'stackoverflow_html_detail',
            '51cto_blog':'51cto_blog_html_detail','codeproject':'codeproject_html_detail','lupaworld':'lupaworld_html_detail','iteye_blog':'iteye_blog_html_detail','gna':'gna_html_detail','apache':'apache_html_detail','phpchina':'phpchina_html_detail','softpedia':'softpedia_html_detail','linuxtone':'linuxtone_html_detail','slashdot':'slashdot_html_detail',
            'neitui':'neitui_html_detail','lagou':'lagou_html_detail'}


for name in websiteInfo.keys():
    print  dicTable[name]


sourceDB={"host":'localhost',"user":'root',"passwd":'root',"port":3306,"extractDB":'test_db',"crawlerDB":"pages"}
SourceConn = MySQLdb.connect(host=sourceDB["host"],user=sourceDB["user"],passwd=sourceDB["passwd"],port=sourceDB["port"])

cur = SourceConn.cursor()
 #choose which database
SourceConn.select_db(sourceDB["crawlerDB"])
tmp = sourceDB["crawlerDB"]
count = cur.execute("SELECT COUNT(DISTINCT url) FROM '%s'","csdn_ask_html_detail")
#count = cur.executemany(sql,value)
print count
result = cur.fetchone()
SourceConn.commit()
"""
'''

cur_time = time.strftime("%Y/%m/%d/%H"+":"+"%M")

websiteInfo = {'openhub_project':{'extract_rate':0.0,'flow_num':0,'flow_rate':0.0,'weekcrawler':0,'crawler_sum':0}}

for name , information in websiteInfo.items():
    flow_num = information['flow_num']
    flow_rate = information['flow_rate']
    extract_rate =  information['extract_rate']
    crawlerSum = information['crawler_sum']
   #condition_values = (flow_num ,flow_rate ,extract_rate , name)
   # logger.info(name +":"+ "flow_num"+":"+str(flow_num) +" "+ "flow_rate"+":" + str(flow_rate)+" "+"extract_rate"+":"+str(extract_rate) )
   #updateTargetDB(updateSql,flow_num,flow_rate,extract_rate,crawlerSum,name)
    if flow_num == 0 or flow_rate <= 0.7:
        content ="the site"+":"+name+" tflow is getting wrong , the charge of tflow is houxiang , please fix it ......."
        emailHandler = EmailHandler.Email(content,cur_time)
        emailHandler.set_desAddr("450717607@qq.com")
        emailHandler.send_email()




# def decorator(func):
#     def wrapper(*args,**kw):
#         func()
#         print('this is decorator')
#         return  func
#     return  wrapper
#
# @decorator

def task():
    print "this is task"

def timer(n):
      while True:
        print time.strftime('%Y-%m-%d %X',time.localtime())
        task()
        time.sleep(n)

if __name__ == '__main__':
    timer(5)
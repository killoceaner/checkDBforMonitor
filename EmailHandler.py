__author__ = 'houxiang'

import smtplib
from email.mime.text import MIMEText
from email.header import Header

class Email(object):


    def __init__(self,content,time):
        self._content = content
        self._time = time

    def set_content(self,content):
        self._content = content

    def set_time(self,time):
        self._time = time

    def set_desAddr(self,desAddr):
        self._desAddr = desAddr

    def send_email(self):
        value = self._time+":"+self._content
        '''
        if len(self._desAddr)==1:
            self.emailSender(value,self._desAddr[0])
        elif len(self._desAddr)>1:
            for addr in self._desAddr:
                self.emailSender(value,addr)
        '''
        des = self._desAddr
        self.emailSender(value,des)



    def emailSender(self ,content , desAddr):
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

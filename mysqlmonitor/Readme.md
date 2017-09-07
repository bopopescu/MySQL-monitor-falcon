#简要说明
mysql_monitor.py文档主要实现了open-falcon中MySQL的监控，监控指标主要包含如下输出：

+ show global status
+ show engine innodb status
+ show master status
+ show slave status
+ select * from information_schema.tables

mysql.info文档是mysql_monitor.py的参数化输入，主要包含两个模块，举例如下：


##[mysql-server]

host = 192.168.10.36      #必填，MySQL的IP地址

port = 3306               #必填，MySQL的监听端口

user = monitor            #必填，MySQL的用户，建议专门创建独立的监控用户

password = 8pRjgYMhgpmIqWg8 #必填，MySQL用户的密码



##[open-falcon]

open_falcon_api = http://172.16.20.117:1988/v1/push   #必填，open-falcon的API地址

step = 60             #必填，建议60秒采样一次

tags = role=mysql     #必填，可以为空

endpoint =            #选填，默认为主机名  


#配置举例
crontab文件放在/etc/cron.d目录下，内容如下（放在salt中统一管理）：

more /etc/cron.d/mysqlmonitor
SHELL=/bin/bash
* * * * * root cd /opt/open-falcon/agent/script/mysql-monitor/ && ./mysql_monitor.py >> /tmp/mysql_monitor.log 2>&1

#常见问题
yum install python2-pip.noarch
yum install mysql-community-devel.x86_64
pip install mysql_python

#关于作者
一个混迹IT届的厨师。

微信：mengxianyao001

邮箱：woshimengxianyao@163.com


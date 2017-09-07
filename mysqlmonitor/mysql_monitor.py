#!/bin/env python
# -*- encoding: utf-8 -*-
# add by mengxy 2017-09-01
# mysql monitor

from __future__ import print_function
from __future__ import division
import MySQLdb
import datetime
import time
import requests
import json
import re
import ConfigParser
import socket
import os


class MySQLMonitorInfo():
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    @property
    def stat_info(self):
        try:

            m = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, port=self.port, charset='utf8',
                                connect_timeout=2)

            query = "SHOW GLOBAL STATUS"
            cursor = m.cursor()
            cursor.execute(query)
            Str_string = cursor.fetchall()

            Status_dict = {}
            for Str_key, Str_value in Str_string:
                Status_dict[Str_key] = Str_value
            cursor.close()
            m.close()

            # 对MySQL的存活的判断依据是show global status是否执行正常
            Status_dict["alive"] = 1
            return Status_dict

        except Exception as e:
            print(datetime.datetime.now())
            print(e)
            Status_dict = {}
            Status_dict["alive"] = 0
            return Status_dict

            # 通过master的信息可以了解到MySQL生成binlog的速度，可以侧面了解TPS和replication的压力

    @property
    def master_info(self):
        try:
            m = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, port=self.port, charset='utf8',
                                connect_timeout=2)
            query = "show master status"
            cursor = m.cursor()
            cursor.execute(query)
            Master_dict = {}

            Str_string = cursor.fetchone()

            if Str_string <> None:
                file, position, _, _, _ = Str_string
                Master_dict['binlog_file_no'] = int(file.split('.')[1])
                Master_dict['binlog_position'] = position

            cursor.close()
            m.close()

            return Master_dict


        except Exception as e:
            print(datetime.datetime.now())
            print(e)
            Master_dict = {}
            return Master_dict

            # 通过slave信息获取延迟的详细信息

    @property
    def slave_info(self):
        try:
            m = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, port=self.port, charset='utf8',
                                connect_timeout=2)
            query = "show slave status"
            cursor = m.cursor()
            cursor.execute(query)
            slave_dict = {}

            Str_string = cursor.fetchone()

            if Str_string <> None:
                slave_dict['Slave_IO_Running'] = Str_string[10]
                slave_dict['Slave_SQL_Running'] = Str_string[11]
                slave_dict['Seconds_Behind_Master'] = Str_string[32]

            cursor.close()
            m.close()
            return slave_dict
        except Exception as e:
            print(datetime.datetime.now())
            print(e)
            slave_dict = {}
            return slave_dict

    @property
    def engine_info(self):
        try:
            m = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, port=self.port, charset='utf8',
                                connect_timeout=2)
            _engine_regex = re.compile(ur'(History list length) ([0-9]+\.?[0-9]*)\n')
            query = "SHOW ENGINE INNODB STATUS"
            cursor = m.cursor()
            cursor.execute(query)
            Str_string = cursor.fetchone()
            a, b, c = Str_string
            cursor.close()
            m.close()
            return dict(_engine_regex.findall(c))

        except Exception as e:
            print(datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            print(e)
            return dict(History_list_length=0)

    @property
    def size_info(self):
        try:
            m = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, port=self.port, charset='utf8',
                                connect_timeout=2)
            query = "SELECT table_schema,sum(DATA_LENGTH) AS data_size,sum(INDEX_LENGTH) as index_size FROM information_schema.tables where table_schema not in ('mysql','information_schema','performance_schema','sys') group by table_schema"
            cursor = m.cursor()
            cursor.execute(query)

            size_dict = {}
            Str_string = cursor.fetchall()

            total_data_size = 0
            total_index_size = 0

            for Str_key, Str_data_size, Str_index_size in Str_string:
                size_dict["schema_datasize_" + Str_key] = int(Str_data_size)
                total_data_size = total_data_size + int(Str_data_size)

                size_dict["schema_indexsize_" + Str_key] = int(Str_index_size)
                total_index_size = total_index_size + int(Str_index_size)

            size_dict["total_datasize"] = int(total_data_size)
            size_dict["total_indexsize"] = int(total_index_size)

            cursor.close()
            m.close()

            return size_dict


        except Exception as e:
            print(datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            print(e)
            size_dict = {}
            return size_dict


if __name__ == '__main__':

    # 获取MySQL的连接信息
    conf = ConfigParser.ConfigParser()

    conf.read(os.getcwd() + "/mysql.info")

    sections = conf.sections()

    options = conf.options('mysql-server')

    host = conf.get("mysql-server", "host")
    port = conf.get("mysql-server", "port")
    user = conf.get("mysql-server", "user")
    password = conf.get("mysql-server", "password")

    # 获取open falcon的配置信息
    open_falcon_api = conf.get("open-falcon", "open_falcon_api")
    step = int(conf.get("open-falcon", "step"))
    tags = conf.get("open-falcon", "tags")

    try:
        endpoint = conf.get("open-falcon", "endpoint")
    except Exception as ConfigParser.NoOptionError:
        endpoint = socket.gethostname()

    timestamp = int(time.time())

    # 获取MySQL的统计信号量

    conn = MySQLMonitorInfo(host, int(port), user, password)

    stat_info = conn.stat_info
    mysql_stat_list = []  # 经过计算整理后的key-value

    if stat_info['alive'] == 0:
        master_info = {}
        engine_info = {}
        slave_info = {}
        size_info = {}
        size_info = {}

        _key = "alive"
        _value = 0

        falcon_format = {
            'Metric': 'gt.mysql.%s' % (_key),
            'Endpoint': endpoint,
            'Timestamp': timestamp,
            'Step': step,
            'Value': _value,
            'CounterType': "GAUGE",
            'TAGS': tags
        }
        mysql_stat_list.append(falcon_format)


    else:

        master_info = conn.master_info
        engine_info = conn.engine_info
        slave_info = conn.slave_info
        size_info = {}
        # 每10分钟采样一次
        if int(timestamp / 60) % 10 == 0:
            size_info = conn.size_info

        monitor_keys = [
            # SQL负载
            ('Com_select', 'COUNTER'),
            ('Qcache_hits', 'COUNTER'),
            ('Com_insert', 'COUNTER'),
            ('Com_update', 'COUNTER'),
            ('Com_delete', 'COUNTER'),
            ('Com_replace', 'COUNTER'),
            ('MySQL_QPS', 'COUNTER'),
            ('MySQL_TPS', 'COUNTER'),
            ('ReadWrite_ratio', 'GAUGE'),

            # Innodb buffer pool
            ('Innodb_buffer_pool_read_requests', 'COUNTER'),
            ('Innodb_buffer_pool_reads', 'COUNTER'),
            ('Innodb_buffer_read_hit_ratio', 'GAUGE'),
            ('Innodb_buffer_pool_pages_flushed', 'COUNTER'),
            ('Innodb_buffer_pool_pages_free', 'GAUGE'),
            ('Innodb_buffer_pool_pages_dirty', 'GAUGE'),
            ('Innodb_buffer_pool_pages_data', 'GAUGE'),

            # SQL流量
            ('Bytes_received', 'COUNTER'),
            ('Bytes_sent', 'COUNTER'),

            # ROW
            ('Innodb_rows_deleted', 'COUNTER'),
            ('Innodb_rows_inserted', 'COUNTER'),
            ('Innodb_rows_read', 'COUNTER'),
            ('Innodb_rows_updated', 'COUNTER'),
            ('Innodb_os_log_fsyncs', 'COUNTER'),
            ('Innodb_os_log_written', 'COUNTER'),

            # 连接
            ('Threads_cached', 'GAUGE'),
            ('Threads_connected', 'GAUGE'),
            ('Threads_created', 'GAUGE'),
            ('Threads_running', 'GAUGE'),

            # 启动时间
            ('Uptime', 'GAUGE'),

            # 杂项
            ('Created_tmp_disk_tables', 'COUNTER'),
            ('Created_tmp_tables', 'COUNTER'),
            ('Connections', 'COUNTER'),
            ('Innodb_log_waits', 'COUNTER'),
            ('Slow_queries', 'COUNTER'),
            ('Binlog_cache_disk_use', 'COUNTER'),
            ('alive','GAUGE')
        ]
        # json格式输出的SHOW global STATUS性能指标

        for _key, falcon_type in monitor_keys:
            if _key == 'MySQL_QPS':
                _value = int(stat_info.get('Com_select', 0)) + int(stat_info.get('Qcache_hits', 0))
            elif _key == 'MySQL_TPS':
                _value = int(stat_info.get('Com_insert', 0)) + int(stat_info.get('Com_update', 0)) + int(
                    stat_info.get('Com_delete', 0)) + int(stat_info.get('Com_replace', 0))
            elif _key == 'Innodb_buffer_read_hit_ratio':
                try:
                    _value = round((int(stat_info.get('Innodb_buffer_pool_read_requests', 0)) - int(
                        stat_info.get('Innodb_buffer_pool_reads', 0))) / int(
                        stat_info.get('Innodb_buffer_pool_read_requests', 0)) * 100, 3)
                except ZeroDivisionError:
                    _value = 0
            elif _key == 'ReadWrite_ratio':
                try:
                    _value = round((int(stat_info.get('Com_select', 0)) + int(stat_info.get('Qcache_hits', 0))) / (
                        int(stat_info.get('Com_insert', 0)) + int(stat_info.get('Com_update', 0)) + int(
                            stat_info.get('Com_delete', 0)) + int(stat_info.get('Com_replace', 0))), 2)
                except ZeroDivisionError:
                    _value = 0
            else:
                _value = int(stat_info.get(_key, 0))

            falcon_format = {
                'Metric': 'gt.mysql.%s' % (_key),
                'Endpoint': endpoint,
                'Timestamp': timestamp,
                'Step': step,
                'Value': _value,
                'CounterType': falcon_type,
                'TAGS': tags
            }
            mysql_stat_list.append(falcon_format)

        # json格式输出show master info的性能指标
        if master_info:
            for _key, _value in master_info.items():
                falcon_format = {
                    'Metric': 'gt.mysql.%s' % (_key),
                    'Endpoint': endpoint,
                    'Timestamp': timestamp,
                    'Step': step,
                    'Value': _value,
                    'CounterType': "GAUGE",
                    'TAGS': tags
                }
                mysql_stat_list.append(falcon_format)

                # json格式输出show slave info的性能指标
        if slave_info:

            if slave_info['Slave_IO_Running'] == 'Yes' and slave_info['Slave_IO_Running'] == 'Yes':
                _key = 'Slave_Running'
                _value = 1

            else:
                _key = 'Slave_Running'
                _value = 0

            falcon_format = {
                'Metric': 'gt.mysql.%s' % (_key),
                'Endpoint': endpoint,
                'Timestamp': timestamp,
                'Step': step,
                'Value': _value,
                'CounterType': "GAUGE",
                'TAGS': tags
            }
            mysql_stat_list.append(falcon_format)

            _key = 'Seconds_Behind_Master'
            _value = slave_info['Seconds_Behind_Master']
            falcon_format = {
                'Metric': 'gt.mysql.%s' % (_key),
                'Endpoint': endpoint,
                'Timestamp': timestamp,
                'Step': step,
                'Value': _value,
                'CounterType': "GAUGE",
                'TAGS': tags
            }
            mysql_stat_list.append(falcon_format)

            # json格式输出的SHOW ENGINE INNODB STATUS性能指标
        if engine_info:
            for _key, _value in engine_info.items():
                _key = "Undo_Log_Length"
                falcon_format = {
                    'Metric': 'gt.mysql.%s' % (_key),
                    'Endpoint': endpoint,
                    'Timestamp': timestamp,
                    'Step': step,
                    'Value': int(_value),
                    'CounterType': "GAUGE",
                    'TAGS': tags
                }
                mysql_stat_list.append(falcon_format)

                # json格式输出show master info的性能指标
        if size_info:
            for _key, _value in size_info.items():
                falcon_format = {
                    'Metric': 'gt.mysql.%s' % (_key),
                    'Endpoint': endpoint,
                    'Timestamp': timestamp,
                    'Step': 600,  # 这个是特殊的
                    'Value': int(_value),
                    'CounterType': "GAUGE",
                    'TAGS': tags
                }
                mysql_stat_list.append(falcon_format)

    # print(json.dumps(mysql_stat_list, sort_keys=True, indent=4))
    r = requests.post(open_falcon_api, data=json.dumps(mysql_stat_list))
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + "  " + r.text)
    print("size_info:%s" % (size_info))

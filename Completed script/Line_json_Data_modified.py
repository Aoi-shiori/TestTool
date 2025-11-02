#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2024-5-22 15:41:13
# @Author  : 郭军
# -*- ecoding: utf-8 -*-
# @ModuleName: Line_json_Data_modified
# @description: 将已有linejson文件数据重新生成固定日期或者指定日期的数据
# @Author: 郭军
# @Time: 2024-6-3 14:57:05
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
from threading import Thread
import jsonlines
import os

def JsonData_read(file):
    # with jsonlines.open(file) as reader:
    #     return reader
    try:
        reader=jsonlines.open(file)
        return reader
    except:
        print("文件打开失败！")

def JsonData_write(readers,new_file):

    with jsonlines.open(new_file, mode="w") as writer:
        for reader in readers:
            writer.write(reader)

def modifiled_json(file, day, sensorsn, subjectID):
    reader=JsonData_read(file)
    try:
        if next(reader.iter())["sensorSn"] != "" and next(reader.iter())["sensorSn"] is not None:
            pass
    except:
        raise ValueError("文件不符合规范！请检查文件是否正确！")

    # 方法一
    date = []
    timestamp=0
    for obj in reader:
        # print("修改前: ",obj)
        if sensorsn == "" or sensorsn is None:
            obj["sensorInfo"] = get_sensorInfo(obj["sensorInfo"], sn=sensorsn, day=day)
        else:
            obj["sensorSn"]= sensorsn
            sn = sensorsn.split("_")[-1]
            obj["sensorInfo"] = get_sensorInfo(obj["sensorInfo"], sn, day)
        obj["recordTime"] = obj["recordTime"] + (86400*day*1000)
        obj["collectTime"] = obj["collectTime"] + (86400*day*1000)
        obj["receiveTime"] = obj["receiveTime"] + (86400*day*1000)
        # obj["timezone"] = "-14400"
        # obj["appId"] = "com.vivalnk.vitalsmonitor"

        # 修改subjectID
        if subjectID == "" or subjectID is None:
            pass
        else:
            obj["subjectId"] = subjectID

        # print("修改后: ",obj)
        date.append(obj)
        timestamp=obj["recordTime"]
    new_file=get_NewFileName(file,timestamp,sensorsn)
    JsonData_write(readers = date,new_file=new_file)

def get_sensorInfo(sensorInfo,sn,day):
    # 将字符串转换为字典
    sensorInfo = json.loads(sensorInfo)
    if sn == "" or sn is None:
        pass
    else:
        # 修改sn
        sensorInfo["sn"] = sn.split("_")[-1]

    # 修改时间戳
    sensorInfo["timeStamp"] = sensorInfo["timeStamp"] + (86400*day)  # 11位时间戳

    # 将字典转换为字符串
    sensorInfo = json.dumps(sensorInfo)

    return sensorInfo

def get_NewFileName(file,timestamp,sensorsn):

    file_name = os.path.basename(file)

    path=os.path.dirname(file)

    try:

        # 分割文件名
        file=file_name.split("_")

        # 预处理文件名
        if file[5] != "denoised.linejson":
            # 去除5后面的内容
            file = file[:5]
            file.append("denoised.linejson")
        else:
            pass

        # 时间处理
        # print(timestamp)
        timestamp=timestamp/1000

        # 设置环境变量TZ为UTC
        os.environ['TZ'] = 'UTC'

        # 获取时间
        human_time = time.strftime('%Y-%m-%d', time.gmtime(timestamp))
        print(human_time)

        if sensorsn.split("_")[-1].split("/")[-1] == "":
            print("sensorSn不符合规范！")
        else:
            # 文件SN修改 "ECGRec_202350/CA80973"
            file[1] = sensorsn.split("_")[-1].split("/")[0]
            file[2] = sensorsn.split("_")[-1].split("/")[-1]
        # 文件时间修改
        file[3]=human_time
        # file[4]="ECG"
        file[5]="denoised.linejson"

        # 获取文件中denoised.linejson的索引 用于插入时间戳或者其他信息
        index=file.index("denoised.linejson")

        # 获取当前时间戳
        current_timestamp = time.time()

        # 插入当前时间戳
        # file.insert(index,str(int(current_timestamp*1000)))

        # 组合成新名称
        separator = '_'
        newfile=separator.join(file)
    except:
        print("文件名修改失败！")

    # 拼接路径
    if path == "":
        newfile=newfile
    else:
        newfile = path + "/" + newfile

    print(newfile)
    return newfile


# 获取间隔天数
def get_Days(times):
    reader = JsonData_read(file)
    try:
        # # 获取文件中第一条数据的事件 方法1
        # for obj in reader:
        #     timestamp_ms=obj["recordTime"]
        #     break
        # timestamp_ms = next(reader)["recordTime"]

        # 获取文件中第一条数据的时间戳 方法2
        reader=reader.iter()
        timestamp_ms = next(reader)["recordTime"]

        print("获取文件第一条数据时间戳：",timestamp_ms)
    except:
        print("文件中没有数据！")

    # 获取文件中第一条数据的事件
    appointed_day = times

    # 将时间戳转换为秒
    timestamp_s = timestamp_ms / 1000

    # 将时间戳转换为datetime对象
    date_string = datetime.datetime.fromtimestamp(timestamp_s)
    date_str = date_string.strftime("%Y-%m-%d")

    # 将字符串转换为datetime对象
    date_str=datetime.datetime.strptime(date_str, "%Y-%m-%d")

    date_end= datetime.datetime.strptime(appointed_day, "%Y-%m-%d")

    # 计算时间差
    delta=date_end-date_str
    print(delta)
    return delta.days


# 判断日期是否合法
def is_valid_date(date_string, format_string):
    try:
        datetime.datetime.strptime(date_string, format_string)
        return True
    except ValueError:
        return False

def main(file, day, sensorsn, subjectID):

    modifiled_json(file, day, sensorsn, subjectID)



if __name__ == '__main__':
    file="D:\\01-WorkSpace\\02-项目\\03-Webportal\\测试用数据\\编辑数据\\ECGRec_202329_E110083_2024-04-23_ECG_denoised.linejson\\ECGRec_202329_E110083_2024-04-23_ECG_denoised_原始.linejson"
    # file="D:/01-WorkSpace/02-项目/03-Webportal/测试用数据/编辑数据/ECGRec_202329_E110083_2024-04-23_ECG_denoised.linejson/ECGRec_202329_E110083_2024-04-23_ECG_denoised.linejson"
    # file="ECGRec_202329_E110083_2024-04-23_ECG_denoised.linejson"

    # 可配置修改项,不修改配置为空
    ## 传感器sn
    sensorsn = "ECGRec_202350/CA80973"
    ## subjectID
    subjectID = "J001"



    # 请选择固定生成还是制定日期生成
    while True:
        try:
            select=int(input("请选择固定生成还是制定日期生成.列如：\n1、输入：1,重新生成固定日期数据.\n2、输入：2,重新生成指定日期数据。\n请输入: "))

            if select == 1:
                while True:
                    try:
                        days = int(input(
                            "请输入要重新生成的天数.列如：\n1、输入：-10,重新生成(包括文件中当前时间)之前10天数据.\n2、输入：10,重新生成(包括文件中当前时间)之后10天数据。\n请输入: "))
                        break
                    except ValueError:
                        print("错误：请输入一个有效的数字。\n")
                starttime = time.time()
                if days < 0:
                    days = abs(days)
                    for day in range(0, days + 1):
                        main(file, -day, sensorsn, subjectID)
                else:
                    days = abs(days)
                    for day in range(0, days + 1):
                        main(file, day ,sensorsn, subjectID)
                print("文件生成完毕！")
                endtime = time.time()
                print("生成耗时：{}".format(endtime - starttime))

            elif select == 2:
                # 请输入指定日期或者字符串
                while True:
                    try:
                        times = input(
                            "请输入指定日期字符串.列如：2024-02-01,生成指定日期文件.\n请输入: ")
                        times = times.replace(" ", "")
                        is_valid = is_valid_date(times, "%Y-%m-%d")
                        if times == "" or is_valid == False:
                            times = None
                            raise ValueError
                        if times is not None:
                            # 记录开始时间
                            starttime = time.time()
                            # 获取间隔天数
                            days = get_Days(times)
                            # 重新生成指定日期数据
                            main(file, days, sensorsn, subjectID)
                            print("文件生成完毕！")
                            # 记录结束时间
                            endtime = time.time()
                            print("生成耗时：{}".format(endtime - starttime))
                            break
                    except ValueError:
                        print("错误：请输入一个有效的日期字符串!!\n")

            else:
                raise ValueError

            break
        except ValueError:
            print("错误：请输入一个有效的数字。")
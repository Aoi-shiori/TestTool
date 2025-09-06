# -*- ecoding: utf-8 -*-
# @ModuleName: bind_Devices_Session
# @Author: Jun
# @Time: 2023年11月27日13:28:49
"""
压测环境
将设备绑定到指定的Tenant下
"""
import sys

import pymysql

# def connect_mysql():
#     db = pymysql.connect(host="test-vcloud.cpkhxr9rjohp.ap-south-1.rds.amazonaws.com",port=3306,
#                          user='rex',password='weq@d234Ffqlk77@dfgh',
#                          database='vcloud_test')
#     return db

# class Database:
#     # def Connect_db(self):
#     #         con = pymysql.connect(host="test-vcloud.cpkhxr9rjohp.ap-south-1.rds.amazonaws.com", port=3306,
#     #                          user='rex', password='weq@d234Ffqlk77@dfgh',
#     #                          database='vcloud_test')
#     #         return con
#     #
#     # def Commit_db(self,sql,values):
#     #     con = self.Connect_db()
#     #     cursor = con.cursor()
#     #     cursor.executemany(self,sql,values)
#     #     con.commit()
#     #     con.close()


class Database:
    def commit_DB(sql,values):
        con = pymysql.connect(host="test-vcloud.cpkhxr9rjohp.ap-south-1.rds.amazonaws.com", port=3306,
                                 user='rex', password='weq@d234Ffqlk77@dfgh',
                                 database='vcloud_test')
        cursor = con.cursor()
        cursor.executemany(sql,values)
        con.commit()
        con.close()


class BindDeviceSession:
    def bindTenantDevice(values, self=None):
        try:
            sql = "INSERT INTO device_tenant (device_id,tenant) VALUES (%s, %s)"
            # values = [("ECGRec_202100/C121212","Second"),("ECGRec_202100/C121213","Second")]
            # cursor.executemany(sql, values)
            Database.commit_DB(sql, values)

        finally:
            print("设备绑定tenant：device_tenant表sql执行完成")


    def bindVcloudSession(values, self=None):
        try:
            sql = "INSERT INTO vcloud_session (device_id,tenant_id,user_id,start_time) VALUES (%s, %s, %s, %s)"
            # values=[('ECGRec_200000/C100000', '458', '350933', '1700643600000')]
            Database.commit_DB(sql, values)

        finally:
            print("设备创建Session：vcloud_session表sql执行完成")

# 清除数据库测试数据
class ClaerData:
    # def __init__(self,values):
    #     self.values=values
    def clearDeviceData(values):
        # print(values)
        try:
            sql = "delete from device_tenant Where tenant=%s"
            # sql = "delete from device_tenant Where device_id=%s and tenant=%s"
            Database.commit_DB(sql,values)
        finally:
            print("device_tenant表，Tenant{}下数据清理完成".format(values))
    def clearSessionData(values):
        try:
            sql = "delete from vcloud_session Where tenant_id=%s and user_id=%s"
            Database.commit_DB(sql,values)
        finally:
            print("vcloud_sessionb数据清理完成")


class CreateDrviceList:

    # 创建Device_tenant表的list
    def create_Device_List(device_count, SN_Patch,tenant_name):
        device_list = []
        if (len(str(int(device_count))) < 6):
            for item in range(int(device_count)):
                lenth=len(str(int(item+old_id)))
                Patch_SN = f"{SN_Patch[0:-lenth]}{str(item+old_id)}"
                device_list.append((Patch_SN, tenant_name))
        else:
            sys.exit("{}:暂时不支持创建那么多设备:{}".format(SN_Patch,int(device_count)))

        return  device_list


    # 创建vcloud_session表的list
    def create_Session_List(device_count, SN_Patch, tenant_id, user_id, start_time):
        device_list = []
        if (len(str(int(device_count))) < 6):
            for item in range(int(device_count)):
                lenth=len(str(int(item+old_id)))
                Patch_SN = f"{SN_Patch[0:-lenth]}{str(item+old_id)}"
                device_list.append((Patch_SN, tenant_id, user_id, start_time))
        else:
            sys.exit("{}:暂时不支持创建那么多设备:{}".format(SN_Patch,int(device_count)))
        return  device_list

    def create_ClearDevice_List(device_count, SN_Patch, tenant_name ):
        device_list = []
        if (len(str(int(device_count))) < 6):
            for item in range(int(device_count)):
                lenth=len(str(int(item+old_id)))
                Patch_SN = f"{SN_Patch[0:-lenth]}{str(item+old_id)}"
                device_list.append((Patch_SN,tenant_name))
        else:
            sys.exit("{}:暂时不支持创建那么多设备:{}".format(SN_Patch,int(device_count)))
        return  device_list






class Total_Patchs:
    def create_Total_Patch_Tenant(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name):
        # 拼接sql中需要的values
        ecg_list =CreateDrviceList.create_Device_List(ecg_count, ECGPatch, tenant_name)
        spo2_list = CreateDrviceList.create_Device_List(spo2_count, SpO2Patch, tenant_name)
        bp_list = CreateDrviceList.create_Device_List(bp_count, BPPatch, tenant_name)
        total_list = ecg_list+spo2_list+bp_list
        return total_list

    def create_Total_Patch_Session(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_id,user_id,start_time ):
        # 拼接sql中需要的values
        ecg_list = CreateDrviceList.create_Session_List(device_count=ecg_count, SN_Patch=ECGPatch, tenant_id=tenant_id,user_id=user_id,start_time=start_time)
        spo2_list = CreateDrviceList.create_Session_List(spo2_count, SpO2Patch, tenant_id,user_id,start_time)
        bp_list = CreateDrviceList.create_Session_List(bp_count, BPPatch, tenant_id,user_id,start_time)
        total_list = ecg_list + spo2_list + bp_list
        return total_list

    def create_Total_Clear_List(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name):
        ecg_list=CreateDrviceList.create_ClearDevice_List(device_count=ecg_count,SN_Patch=ECGPatch,tenant_name=tenant_name)
        spo2_list=CreateDrviceList.create_ClearDevice_List(device_count=spo2_count,SN_Patch=SpO2Patch,tenant_name=tenant_name)
        bp_list=CreateDrviceList.create_ClearDevice_List(device_count=bp_count,SN_Patch=BPPatch,tenant_name=tenant_name)
        total_list=ecg_list+spo2_list+bp_list
        return total_list






def main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name,tenant_id,user_id,start_time):






    # device_tenant
    total_list_tenant = Total_Patchs.create_Total_Patch_Tenant(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name)

    # vcloud_session
    total_list_session = Total_Patchs.create_Total_Patch_Session(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_id,user_id,start_time)

    # total_list_clear = Total_Patchs.create_Total_Clear_List(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name)

    # 数据清理
    # ClaerData.clearSessionData(values=[(tenant_id,user_id)])
    # ClaerData.clearDeviceData(values=[tenant_name])

    # ClaerData.clearDeviceData(total_list_clear)

    # 绑定设备
    # insert device_tenant
    BindDeviceSession.bindTenantDevice(total_list_tenant)

    # insert vcloud_session
    # BindDeviceSession.bindVcloudSession(total_list_session)

if __name__ == '__main__':
    # 当前已经创建到的结尾id
    old_id=0
    # 要绑定的ECG数量
    ecg_count = 200
    # ECG设备号
    ECGPatch = "ECGRec_100000/C100000"
    # 要绑定的SpO2数量
    spo2_count =0
    # SpO2  设备号
    SpO2Patch = "O2 1000000000"
    # 要绑定的BP数量
    bp_count = 0
    # BP  设备号
    BPPatch = "BP5S_1000000000000"
    tenant_name = "Second"
    # 项目id
    tenant_id = "458"
    # 用户id/subjectid  用户J001
    user_id = "350933"
    # 2023-12-05 10:00:00
    start_time = 1701741600000

    main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name,tenant_id,user_id,start_time)

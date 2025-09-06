# -*- ecoding: utf-8 -*-
# @ModuleName: bind_devices
# @Author: Rex
# @Time: 2023/11/21 15:13
"""
压测环境
将设备绑定到指定的Tenant下
"""
import pymysql

def connect_mysql():
    db = pymysql.connect(host="test-vcloud.cpkhxr9rjohp.ap-south-1.rds.amazonaws.com",port=3306,
                         user='rex',password='weq@d234Ffqlk77@dfgh',
                         database='vcloud_test')
    return db

def bind_tenant_device(values):
    try:
        db = connect_mysql()
        cursor = db.cursor()
        sql = "INSERT INTO device_tenant (device_id,tenant) VALUES (%s, %s)"
        # values = [("ECGRec_202100/C121212","Second"),("ECGRec_202100/C121213","Second")]
        cursor.executemany(sql, values)
        db.commit()
    finally:
        cursor.close()
        db.close()

def create_ecg(ecg_count,ECGPatch,tenant_name):
    # 获取ECG设备号/后的值
    ECG_list = []
    for item in range(ecg_count):
        if item < 10:
            ECG_Patch = f"{ECGPatch[0:-1]}{str(item)}"
            ECG_list.append( (ECG_Patch,tenant_name))
        elif ecg_count<100:
            ECG_Patch = f"{ECGPatch[0:-2]}{str(item)}"
            ECG_list.append( (ECG_Patch,tenant_name))
        elif ecg_count<1000:
            ECG_Patch = f"{ECGPatch[0:-3]}{str(item)}"
            ECG_list.append( (ECG_Patch,tenant_name))
        elif ecg_count<10000:
            ECG_Patch = f"{ECGPatch[0:-4]}{str(item)}"
            ECG_list.append( (ECG_Patch,tenant_name))
        else:
            print("暂不支持这么多设备")
    return ECG_list

def create_spo2(spo2_count, SpO2Patch, tenant_name):
    # 获取ECG设备号/后的值
    SpO2_list = []
    for item in range(spo2_count):
        if item < 10:
            SpO2_Patch = f"{SpO2Patch[0:-1]}{str(item)}"
            SpO2_list.append( (SpO2_Patch,tenant_name))
        elif ecg_count<100:
            SpO2_Patch = f"{SpO2Patch[0:-2]}{str(item)}"
            SpO2_list.append( (SpO2_Patch,tenant_name))
        elif ecg_count<1000:
            SpO2_Patch = f"{SpO2Patch[0:-3]}{str(item)}"
            SpO2_list.append( (SpO2_Patch,tenant_name))
        elif ecg_count<10000:
            SpO2_Patch = f"{SpO2Patch[0:-4]}{str(item)}"
            SpO2_list.append( (SpO2_Patch,tenant_name))
        else:
            print("暂不支持这么多设备")
    return SpO2_list

def create_bp(bp_count, BPPatch, tenant_name):
    # 获取ECG设备号/后的值
    BP_list = []
    for item in range(bp_count):
        if item < 10:
            BP_Patch = f"{BPPatch[0:-1]}{str(item)}"
            BP_list.append( (BP_Patch,tenant_name))
        elif ecg_count<100:
            BP_Patch = f"{BPPatch[0:-2]}{str(item)}"
            BP_list.append( (BP_Patch,tenant_name))
        elif ecg_count<1000:
            BP_Patch = f"{BPPatch[0:-3]}{str(item)}"
            BP_list.append( (BP_Patch,tenant_name))
        elif ecg_count<10000:
            BP_Patch = f"{BPPatch[0:-4]}{str(item)}"
            BP_list.append( (BP_Patch,tenant_name))
        else:
            print("暂不支持这么多设备")
    return BP_list

def create_total_patch(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name):
    # 拼接sql中需要的values
    ecg_list =create_ecg(ecg_count, ECGPatch, tenant_name)
    spo2_list = create_spo2(spo2_count, SpO2Patch, tenant_name)
    bp_list = create_bp(bp_count, BPPatch, tenant_name)
    total_list = ecg_list+spo2_list+bp_list
    return total_list


def main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name):
    total_list = create_total_patch(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name)
    bind_tenant_device(total_list)

if __name__ == '__main__':
    # 要绑定的ECG数量
    ecg_count = 1000
    # ECG设备号
    ECGPatch = "ECGRec_200000/C100000"
    # 要绑定的SpO2数量
    spo2_count =1000
    # SpO2  设备号
    SpO2Patch = "O2 1000000000"
    # 要绑定的BP数量
    bp_count = 1000
    # BP  设备号
    BPPatch = "BP5S_1000000000000"
    tenant_name = "Second"
    main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,tenant_name)

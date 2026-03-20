# -*- ecoding: utf-8 -*-
# @ModuleName: main
# @Author: Rex
# @Time: 2021/11/1 2:44 下午
import json
from VivaLNK_ProcessMedicalFile import ProcessISHNE
from logger import logger


def print_msg(msg):
    # print(msg)
    logger.info(msg)

def INSHE_ecg(file_Path):
    """
    读取INSHE文件，并返回一个有ecg组成的数组（数据缺失会补8700）
    :param file_Path:
    :return:
    """
    INSHE_ecg_list = []
    ecg_list = ProcessISHNE(print_msg).read_iSHNE_data(file_Path)
    for ecg in ecg_list:
        # 过滤8700补值
        # if 8700 in ecg['ecg']:
        #     pass
        # else:
        INSHE_ecg_list.append(ecg['ecg'])
    # print(len(INSHE_ecg_list))
    # with open("./st.json", 'w') as  file:
    #     json.dump(INSHE_ecg_list,file)

    return INSHE_ecg_list


def Json_ecg(file_Path):
    """
    读取JSON文件并返回由ecg组成的数组
    :param file_Path:
    :return:
    """
    Json_ecg_list = []
    with open(file_Path, 'r')as f:
        file = json.load(f)
    for ecg in file:
        Json_ecg_list.append(ecg['vitals']['ecg'])
        # Json_ecg_list.append(ecg['ecg'])
    return Json_ecg_list

def deal_Ishne_file_list(ISHNE_file_path):
    INSHE_ecg_list = []
    ecg_list = ProcessISHNE(print_msg).read_iSHNE_data(ISHNE_file_path)
    # print(len(ecg_list))
    for ecg in ecg_list:
        # 过滤8700补值
        # if 8700 in ecg['ecg']:
        #     pass
        # else:
        INSHE_ecg_list.append(ecg['ecg'])
    # print(INSHE_ecg_list)
    # with open("./st.json", 'w') as  file:
    #     json.dump(INSHE_ecg_list,file)

    return INSHE_ecg_list




def main(ISHNE_file_path, ISHNE_all):
    """
    INSHE文件中的ecg与Json文件中ecg对比
    若一致反返回：TRUE
    若不一致返回：FALSE
    :param INSHE_file:
    :param JSON_file:
    :return:
    """
    ISHNE1=deal_Ishne_file_list(ISHNE_file_path)
    # print(len(ISHNE1))
    # print(ISHNE1[0])
    logger.info(len(ISHNE1))
    logger.info((ISHNE1[0]))
    # for item in ISHNE1:
    #     if len(item)!=128:
    #         print(item)
    ISHNE = INSHE_ecg(ISHNE_all)
    # print(len(ISHNE))
    # print(ISHNE[0])
    logger.info(len(ISHNE))
    logger.info((ISHNE[0]))


    # for i in INSHE_list:
    #     print(i)
    # for i in Json_list:
    #     print(i)
    # if ISHNE1 == ISHNE:
    #     print("True")
    # else:
    #     print("False")
    # else:
    # for i in range(len(ISHNE1)):
    #     if ISHNE1[i] == ISHNE[i]:
    #         pass
        # else:
        #     print(i)
        #     print(i)



if __name__ == '__main__':
    # INSHE_ecg("/Users/rexren/2vivalnk脚本库/Check S3 file/INSHEToJson/ECGRec_202128_C766122_1698199175750.ecg")
    main('J20260318001-ECGRec_202603_J031801-2026-02-15_UTC+0800.ecg',
         'J20260318001-ECGRec_202603_J031801-2026-02-15_UTC+0800.ecg')
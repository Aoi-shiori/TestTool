import json
from logger import logger
def getEcgData():
    ecg_list=[]
    hr_list=[]
    rr_list=[]
    # rmssd_list=[]
    # with open("compare-5.json", 'r') as f:
    with open("d004-1.json", 'r') as f:
        message=json.load(f)
        logger.info(f"message:{len(message)}")
        for item in message:
            ecg_list.append(item['ecg'])
            hr_list.append(item['hr'])
            rr_list.append(item['rr'])
            # rmssd_list.append(item['rmssd'])
        return ecg_list,hr_list,rr_list
        # return ecg_list

def getEcgData1():
    data_list = []
    # rmssd_list=[]
    with open("d004-1.json", 'r') as f:
        message = json.load(f)
        for item in message:
            data_list.append({"ecg": item['ecg'], "hr": item['hr'], "rr": item['rr']})
        return data_list
def getEcgData2():
    data_list=[]
    # rmssd_list=[]
    with open("d004-2.json", 'r') as f:
        message=json.load(f)
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData3():
    data_list=[]
    # rmssd_list=[]
    with open("d004-3.json", 'r') as f:
        message=json.load(f)
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData4():
    data_list=[]
    # rmssd_list=[]
    with open("d004-4.json", 'r') as f:
        message=json.load(f)
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData5():
    data_list=[]
    # rmssd_list=[]
    with open("d004-5.json", 'r') as f:
        message=json.load(f)
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData6():
    data_list=[]
    # rmssd_list=[]
    with open("d004-6.json", 'r') as f:
        message=json.load(f)
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData7():
    data_list=[]
    # rmssd_list=[]
    with open("d004-7.json", 'r') as f:
        message=json.load(f)
        # print(len(message))
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData8():
    data_list=[]
    # rmssd_list=[]
    with open("d004-2.json", 'r') as f:
        message=json.load(f)
        # print(len(message))
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData9():
    data_list=[]
    # rmssd_list=[]
    with open("d004-9.json", 'r') as f:
        message=json.load(f)
        # print(len(message))
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData10():
    data_list=[]
    # rmssd_list=[]
    with open("d005.json", 'r') as f:
        message=json.load(f)
        # print(len(message))
        for item in message:
            data_list.append({"ecg":item['ecg'],"hr":item['hr'],"rr":item['rr']})
        return data_list
def getEcgData11():
    data_list=[]
    # rmssd_list=[]
    with open("Alert_Case.json", 'r') as f:
        message=json.load(f)
        # print(len(message))
        for item in message:
            data_list.append(item)
        return data_list
if __name__ == '__main__':
    getEcgData()

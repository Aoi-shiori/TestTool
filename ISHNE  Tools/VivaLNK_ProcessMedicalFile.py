#!/usr/bin/env python 
# coding:utf-8
from VivaLNK_ISHNEHolterLib import Holter,Lead
import datetime
import time
import json
import numpy
import os
import wfdb
from logger import logger


class ProcessISHNE():
    def __init__(self,ishne_log):
        self.ishne_log = ishne_log

    def read_file_data(self):
        all_data = []
        first_time = 0
        with open('/Users/cyanxu/Desktop/C5/VVDeviceDataLog 2021-02-19 16-53.log') as  file_to_read:
            while True:
                lines = file_to_read.readline()
                if not lines:
                    break

                index = lines.index("{")
                lines = lines[index:len(lines)]
                json_dict = json.loads(lines)
                ecg_array = json_dict['ecg']
                if first_time == 0:
                    first_time = json_dict['recordTime']
                for ecg_value in ecg_array:
                    ecg_float_value = ecg_value / 1000.0
                    all_data.append(ecg_float_value)

        ishne_file_name = '/Users/cyanxu/Desktop/C5/'+str(first_time)+'.log'
        self.generate_ishne_format_file(all_data, first_time,ishne_file_name)

    def generate_ishne_format_file(self,ecg_data_list, first_time,output_file_path):

        x = Holter(filename=output_file_path,read_file=False)
        # print(x)
        logger.info(x)
        # print(dir(x))
        logger.info(dir(x))
        x.first_name = 'Xu'
        x.last_name = 'Cyan'
        x.id = ''
        x.file_version = 1
        x.sex = 1
        x.race = 3
        x.pm = 0
        # x.load_data()
        x.sr = 128
        x.ecg_size = len(ecg_data_list)
        x.lead = [None for _ in range(1)]
        x.lead[0] = Lead(qual=1, spec=0, res=1000)
        x.lead[0].data = numpy.array(ecg_data_list, dtype=numpy.float64)
        x.lead[0].res = 1000
        x.lead = [x.lead[0]]
        x.birth_date = datetime.datetime.fromtimestamp(first_time / 1000).date()
        x.record_date = datetime.datetime.fromtimestamp(first_time / 1000).date()
        x.start_time = datetime.datetime.fromtimestamp(first_time / 1000).time()
        x.nleads = 1
        x.recorder_type = bytes('digital', encoding="utf8")
        x.proprietary = 'Proprietary'
        x.copyright = 'Copyright VivaLNK 2019'
        x.reserved = 'This is reserved block'
        x.var_block = bytes('This is variable-length block', encoding="utf8")
        x.write_file(overwrite=True)
        self.ishne_log(f'file {output_file_path} output')

    def read_iSHNE_data(self,ishne_file_path):
        x = Holter(filename=ishne_file_path,read_file=True)
        x.load_data()
        # 切割数据为128长度的数组
        ishne_ecg_array = x.lead[0].data
        ishne_split_ecg_buffer = [ishne_ecg_array[i:i + x.sr] for i in range(0, len(ishne_ecg_array), x.sr)]
        #
        start_time_str = str(x.record_date) +' '+ str(x.start_time)
        # print(f"开始时间UTC0:{start_time_str}")
        logger.info(f'数据的起始时间:{start_time_str}')
        timeArray = time.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        # start_time_stamp = int(time.mktime(timeArray))*1000
        # print(start_time_stamp)
        # print(timeArray)
        # print(start_time_stamp)

        # self.ishne_log('数据的起始时间:'+start_time_str)
        # # 组装完整可绘制图形的数据
        data_buffer = []
        for ecg_buffer in ishne_split_ecg_buffer:
            data_dict = {}
            ecg_array = []
            # start_time_stamp += 1000
            for ecg in ecg_buffer:
                ecg_value = round(ecg /int(x.ampl_res[0])*1000000)
                # ecg_value = round(ecg * -0.034/1000000)
                # ecg_value = ecg * 1000
                ecg_array.append(ecg_value)

            data_dict['ecg'] = ecg_array
            # data_dict['recordTime'] = start_time_stamp
            data_buffer.append(data_dict)

        return data_buffer


class ProcessMitData():
    def __init__(self,mit_log):
        self.mit_log = mit_log

    def read_mit16_data(self,mit16_path):
        '''生成MIT16文件
        :param mit16_path: 携带dat和hea的路径名称
        :return:
        '''
        mit16_path_name = mit16_path.replace('.dat', '')
        mit16_path_name = mit16_path_name.replace('.hea', '')
        record = wfdb.rdrecord(mit16_path_name)
        # print(record.__dict__)
        logger.info(record.__dict__)
        start_date_time = record.base_datetime
        self.mit_log(start_date_time)
        # 生成文件的起始时间
        start_time_stamp = int(time.mktime(start_date_time.timetuple()))*1000

        mit_ecg_buffer = record.p_signal
        mit_split_ecg_buffer = [mit_ecg_buffer[i:i+128] for i in range(0, len(mit_ecg_buffer), 128)]

        data_buffer = []
        for ecg_buffer in mit_split_ecg_buffer:
            data_dict = {}
            ecg_array = []
            start_time_stamp += 1000
            for ecg in ecg_buffer:
                ecg_value = ecg[0]*1000
                ecg_array.append(ecg_value)
            data_dict['ecg'] = ecg_array
            data_dict['recordTime'] = start_time_stamp

            data_buffer.append(data_dict)

        return data_buffer

    def generate_mit_file(self, record_name, sample_frequency, amplitudes, ecg_list, start_record_mills, tzinfo,output_dir_path):
        if not ecg_list or len(ecg_list) == 0:
            return []
        channels = len(ecg_list)
        units = ['mV'] * channels
        signal_name = ['I'] * channels
        adc_gains = amplitudes
        formats = ['16'] * channels
        baselines = [0] * channels
        digital_signals = []
        for index in range(len(ecg_list[0])):
            arr = []
            for channel_index in range(channels):
                arr.append(ecg_list[channel_index][index])
            digital_signals.append(arr)
        if len(digital_signals) > 0:
            base_datetime = datetime.datetime.fromtimestamp(start_record_mills / 1000)
            base_date = base_datetime.date()
            base_time = base_datetime.time()
            wfdb.wrsamp(record_name, sample_frequency, units, signal_name, d_signal=numpy.array(digital_signals),
                        fmt=formats, adc_gain=adc_gains, baseline=baselines, base_date=base_date, base_time=base_time,
                        write_dir=output_dir_path)
            header_file_path = os.path.join(output_dir_path, record_name + '.hea')
            data_file_path = os.path.join(output_dir_path, record_name + '.dat')
            self.mit_log('mit16 file generation path: ' + header_file_path + ' ' + data_file_path)



def print_msg(msg):
    # print(msg)
    logger.info(msg)

if __name__ == '__main__':
    # process_ishne = ProcessISHNE(ishne_log=print_msg)
    # # process_ishne.read_file_data()
    # data=[]
    # process_ishne.generate_ishne_format_file(data,1644910200000,"./")
    def print_msg(msg):
        # print(msg)
        logger.info(msg)
    file="./ECGRec_202150_C823468_2022-12-12_ECG_raw.ecg"
    h=ProcessISHNE(print_msg)
    h.read_iSHNE_data(file)




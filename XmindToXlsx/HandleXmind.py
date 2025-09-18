#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Project: TestTool
@Author: guojun
@Email: 391350540@qq.com
@Date: 2025/9/17 11:58
@File: HandleXmind.py
@IDE: PyCharm
@Description: 
"""

import xmindparser
import json
import os
import zipfile
import tempfile
import shutil
from logger import logger as logger
from PIL import Image as PILImage
import io
import base64


class HandleXmind():
    def __init__(self, xmind_file):
        self.xmind_file = xmind_file
        self.firstSheetName = None
        self.sheetNames = []
        self.case_list = []
        self.case_lists = []
        self.maxModule = 0
        self.image_data = {}  # 存储图片数据 {image_id: image_data}

    def __str__(self, *args, **kwargs):
        logger.info(f'用例数据解析完成 总计模块：{len(self.case_lists)}')
        for case in self.case_lists:
            logger.info(f'模块：{case["title"]} 用例数量：{len(case["Case"])}')
        return ""

    def extract_images_from_xmind(self):
        """从XMind文件中提取所有图片"""

        try:
            # XMind文件实际上是zip压缩包
            with zipfile.ZipFile(self.xmind_file, 'r') as z:
                # 获取所有文件列表
                file_list = z.namelist()
                # logger.info(f"图片文件列表：{file_list}")

                # 查找图片文件
                image_files = [f for f in file_list if f.startswith('resources/') and f != 'resources/']

                # logger.info(f"{image_files}")

                # 提取图片
                for image_file in image_files:
                    try:
                        # 读取图片数据
                        image_data = z.read(image_file)

                        # 获取图片ID（文件名）
                        image_id = os.path.basename(image_file)

                        # 存储图片数据
                        self.image_data[image_id] = image_data

                        logger.info(f"提取图片: {image_id}")
                    except Exception as e:
                        logger.error(f"提取图片失败 {image_file}: {str(e)}")

        except Exception as e:
            logger.error(f"解压XMind文件失败: {str(e)}")

    def handle_xmind(self):
        """解析XMind文件"""
        # 首先提取图片
        self.extract_images_from_xmind()

        # 然后解析XMind内容
        dict_data = xmindparser.xmind_to_dict(self.xmind_file)
        # logger.info(f"完整用例解析数据{dict_data}")

        all_data = []
        for i in dict_data:
            topic = i['topic']
            title = topic['title']
            topics = topic.get('topics', [])

            # 将数据组装放入列表
            data_dict = {"title": title, "topics": topics}
            all_data.append(data_dict)

        # xmind内容主题，取第一个主题，可用该名字作为最后Excel报告的文件名
        self.firstSheetName = all_data[0]['title']

        # 每个画布的xmind主题内容,用该名字创建sheet
        for i in all_data:
            self.sheetNames.append(i['title'])

        # 处理所有数据
        for data in all_data:
            new_case_list = []
            self.get_all_topic_data(data.get('title'), data.get('topics', []), {})

            for case in self.case_list:
                new_dice_case = {"module-1": data.get('title')}
                new_dice_case.update(case)
                new_case_list.append(new_dice_case)

            self.case_list = new_case_list
            case_dict = {"title": data.get('title'), "Case": self.case_list}
            self.case_list = []
            self.case_lists.append(case_dict)

        # 打印所有用例数据
        logger.info(self.__str__())

    def get_all_topic_data(self, title, data, dic):
        """拿到所有topic的数据，并传递有效的case值 type dict"""
        dict_case = dic

        if len(data) == 1:
            dict_data = data[0]
            self.get_title_data(title, dict_case, dict_data)
        else:
            for i in range(len(data)):
                if i == 0:
                    self.get_title_data(title, dic.copy(), data[i])
                else:
                    if "makers" in data[i] and data[i]["makers"]:
                        if data[i]['makers'] == ['priority-1']:
                            new_dict_case = {}
                            self.get_title_data(title, new_dict_case, data[i])
                        else:
                            new_dict_case = dict_case.copy()
                            self.get_title_data(title, new_dict_case, data[i])
                    elif "标题" in data[i]['title'] and ("步骤" in data[i]['title'] or "预期" in data[i]['title']):
                        new_dict_case = dict_case.copy()
                        self.get_title_data(title, new_dict_case, data[i])
                    elif "topics" in data[i]:
                        new_dict_case = dict_case.copy()
                        self.get_all_topic_data(title, data[i]['topics'], new_dict_case)

    def get_title_data(self, title, dict_case, dict_data):
        """处理数据，并拿到所有需要数据的titile值"""
        if "title" in dict_case:
            new_dict_case = dict_case.copy()

            if "makers" in dict_data and "标题" not in dict_data['title']:
                makers = dict_data['makers']
                if makers == ['priority-1']:
                    new_dict_case['module-1'] = dict_data['title']
                    self.check_max_module(1)
                elif makers == ['priority-2'] or 'priority-2' in makers:
                    new_dict_case['module-2'] = dict_data['title']
                    self.check_max_module(2)
                elif makers == ['priority-3'] or 'priority-3' in makers:
                    new_dict_case['module-3'] = dict_data['title']
                    self.check_max_module(3)
                elif makers == ['priority-4'] or 'priority-4' in makers:
                    new_dict_case['module-4'] = dict_data['title']
                    self.check_max_module(4)
                elif makers == ['priority-5'] or 'priority-5' in makers:
                    new_dict_case['module-5'] = dict_data['title']
                    self.check_max_module(5)


            if "标题" in dict_data['title'] and ("期望" in dict_data['title'] or "预期" in dict_data['title']):
                case = dict_data['title']
                self.case_format(new_dict_case, case)
                self.set_case_status(new_dict_case, dict_data)
                self.get_case_note_labels(new_dict_case, dict_data)
                self.get_case_image(new_dict_case, dict_data)
                logger.info(f'解析后1：{new_dict_case}')
                self.case_list.append(new_dict_case)

            if "topics" in dict_data:
                self.get_all_topic_data(title, dict_data['topics'], new_dict_case)
        else:
            if "makers" in dict_data and "标题" not in dict_data['title']:
                makers = dict_data['makers']
                if makers == ['priority-1']:
                    dict_case['module-1'] = dict_data['title']
                    self.check_max_module(1)
                elif makers == ['priority-2'] or 'priority-2' in makers:
                    dict_case['module-2'] = dict_data['title']
                    self.check_max_module(2)
                elif makers == ['priority-3'] or 'priority-3' in makers:
                    dict_case['module-3'] = dict_data['title']
                    self.check_max_module(3)
                elif makers == ['priority-4'] or 'priority-4' in makers:
                    dict_case['module-4'] = dict_data['title']
                    self.check_max_module(4)
                elif makers == ['priority-5'] or 'priority-5' in makers:
                    dict_case['module-5'] = dict_data['title']
                    self.check_max_module(5)

            try:
                if "标题" in dict_data['title'] and ("期望" in dict_data['title'] or "预期" in dict_data['title']):
                    # logger.info(f"解析用例--------00{ dict_data['title']}")
                    # logger.info(f"解析用例--------00{dict_data['topics']}")
                    if "title" in dict_case:
                        new_dict_case = dict_case.copy()
                        case = dict_data['title']
                        self.case_format(new_dict_case, case)
                        self.set_case_status(new_dict_case, dict_data)
                        self.get_case_note_labels(new_dict_case, dict_data)
                        self.get_case_image(new_dict_case, dict_data)
                        logger.info(f'解析后2：{new_dict_case}')
                        self.case_list.append(new_dict_case)
                    else:
                        case = dict_data['title']
                        self.case_format(dict_case, case)
                        self.set_case_status(dict_case, dict_data)
                        self.get_case_note_labels(dict_case, dict_data)
                        self.get_case_image(dict_case, dict_data)
                        # logger.info(f'解析后3：{dict_case}')
                        self.case_list.append(dict_case)
            except Exception as e:
                logger.error(f'处理用例失败: {dict_case}, {dict_data}, 错误: {str(e)}')

            if "topics" in dict_data:
                self.get_all_topic_data(title, dict_data['topics'], dict_case)

    # 其他方法保持不变（set_case_status, get_case_note_labels, check_max_module, case_format）
    # 这里省略了这些方法的代码，您需要保留原有的实现

    def set_case_status(self, dict_case, dict_data):
        """
        解析xmind中实际的case数据，并根据实际情况赋予case状态值
        :param dict_case:
        :param dict_data:
        :return:
        """
        if list(dict_data.keys()).__contains__("makers"):
            maker = dict_data['makers']
            if maker.__contains__("task-done"):
                dict_case['case_status'] = "PASS"
                # 兼容mac&windows版本
            elif maker.__contains__("symbol-attention") or maker.__contains__("symbol-exclam"):
                dict_case['case_status'] = "FAIL"

            else:
                dict_case['case_status'] = "N/A"

            # 增加用例优先级的判断，作用是否要用于回归
            if maker.__contains__("priority-1"):
                dict_case['regression'] = "1"
            elif maker.__contains__("priority-2"):
                dict_case['regression'] = "2"
            elif maker.__contains__("priority-3"):
                dict_case['regression'] = "3"
            else:
                dict_case['regression'] = "N/A"
            # 判断用例是 1删除 2更新 3新增
            if maker.__contains__("tag-grey"):
                dict_case['status'] = "1"
            elif maker.__contains__("tag-blue"):
                dict_case['status'] = "2"
            elif maker.__contains__("tag-orange"):
                dict_case['status'] = "3"
            else:
                dict_case['status'] = ""



        else:
            dict_case['case_status'] = "N/A"

    def get_case_note_labels(self, dict_case, dict_data):
        """
        处理case中的note和labels数据，一般该数据是指case失败后的备注
        :param dict_case:
        :param dict_data:
        :return:
        """
        if list(dict_data.keys()).__contains__("note"):
            note = dict_data['note']
            dict_case['note'] = note
        elif list(dict_data.keys()).__contains__("labels"):
            labels = dict_data['labels']
            str_labels = ",".join(labels)
            dict_case['note'] = str_labels
        else:
            dict_case['note'] = ""

    def get_nested_value(self,data, *keys, default=None):
        """安全获取嵌套字典值的通用函数"""
        current = data
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    def get_image_src(self,topic):
        # 首先检查 topic 是否为字典
        if not isinstance(topic, dict):
            return ""

        # 获取 image 值
        image = topic.get("image")

        # 如果 image 为 None，返回空字符串
        if image is None:
            return ""

        # 如果 image 是字典，尝试获取 src
        if isinstance(image, dict):
            return image.get("src", "")

        # 如果 image 是其他类型（如字符串），返回空字符串
        return ""


    def get_case_image(self,dict_case, dict_data):
        """
        {'note': '20250915193847', 'title': '标题：V2时区 - 检查病人"Patient Profile"页面“Timezone”参数项可编辑无提示信息
前置：
1.当前账号有权限进入单病人“Patient Profile”页面
2.在"Clinic"中配置“Patient Timezone Behavior ”参数为"V2"

步骤：
1.检查 "Timezone"参数项
2.检查 "Timezone"参数项提示信息
2.检查推送数据有时区名，时区更新逻辑
3.检查推送数据无时区名时，时区更新逻辑


预期：
1.红色“*”标记必填项，不可编辑。
2.有"Tooltip"提示信息：“This timezone reflects the patient’s most recent location, derived from app-uploaded data or the local timezone during manual entry, and updates automatically when changes are detected.”，有“Last Change”信息：格式 “YYYY-MM-DD hh:mm:ss (UTC±08:00)”（基于浏览器本地时区显示）。
3.取推送数据处理完成后，最后一条数据的时区名称进行更新。
4.推送数据处理完成后，最后一条数据时区名为空，则不进行更新操作。', 'topics': [{'makers': ['task-done'], 'title': '添加 BP', 'topics': [{'image': {'src': 'xap:resources/793e66c34c2470cb918f62f9a4260d77a2cb12c06b3f547a46a3701a8b5f3a17.png'}, 'title': '不更新病人时区'}, {'image': {'src': 'xap:resources/2bea0cea16bc9967c65ea9bbc7a2b96daa337d2bd4e7427283445598f63546e6.png'}, 'title': '夏令时时区，偏移量为数据开始时间时偏移量'}, {'image': {'src': 'xap:resources/1406d4dc11ddc0d54c167e416c16291639bd3c871300c3bdb0b9771bd1cae524.png'}, 'title': '细分主题 3'}, {'image': {'src': 'xap:resources/e5b586c71312610d15a28a7bb6276ff62bbbd7c84c229ab5712da164cc85acae.png'}, 'title': '细分主题 5'}, {'image': {'src': 'xap:resources/1746d05656cbd4bf8fa3182b3a4f353609a49ed56e1d7ca2a7d15c9f614edc4e.png'}, 'title': '夏令时时区，偏移量为数据开始时间时偏移量'}, {'image': {'src': 'xap:resources/b99829b37d8272dd352036c7dc1d432a2fc53a686e306263243462641795e5eb.png'}, 'title': '细分主题 6'}, {'image': {'src': 'xap:resources/a7dd5951ec1c5e3ecd678974867840204225668f00a0341da06e54252c0c83e8.png'}, 'title': 'Day Totals'}, {'image': {'src': 'xap:resources/7bc9e55a4c4d53cfa0db602afa7b16c89d651d81776fc6cba9a483eafd5d81b7.png'}, 'title': 'Day Totals'}, {'image': {'src': 'xap:resources/006d37e18393c95ff28e554c59bd9bc8fab00f70bbab3a6c7a986a66b0073a7a.png'}, 'title': '日期选择控件显示'}, {'image': {'src': 'xap:resources/689f5af1efb508ba922eafcaea1bc85cca41bfab1d5b05d1e6dc9e9db65e03e4.png'}, 'title': '日期选择控件显示'}]}, {'title': '细分主题 2'}]}
        Parameters
        ----------
        dict_case
        dict_data

        Returns
        -------

        """
        # 读取到的图片数据
        image_data_list = []
        # 检查是否有图片
        result = self.get_nested_value(dict_data, 'topics')
        if result:
            # 获取主题下 topics 种所有图片
            _dict_data= dict_data['topics']
            for topic in _dict_data:
                if "image" in topic:
                        # try:
                        #     _image = (_topic.get('image') or {}).get('src', '')
                        # except:
                        #     _image =
                        #     ''
                        result = self.get_image_src(topic)
                        image_data = {"image": result, "title": topic.get('title', ""),"label": topic.get('label', "")}
                        if image_data.get("image") != "":
                            image_data["image"] = image_data["image"].split("/")[-1]
                        image_data_list.append(image_data)
                elif "topics" in topic and "image" not in topic:
                    _topics_data = topic['topics']
                    for _topic in _topics_data:
                        if "image" in _topic:
                            result = self.get_image_src(_topic)
                            image_data ={"image": result, "title": _topic.get('title',""), "label": _topic.get('label',"")}
                            if image_data.get("image") != "":
                                image_data["image"] = image_data["image"].split("/")[-1]

                            image_data_list.append(image_data)
                else:
                    # try:
                    #     image = (topic.get('image') or {}).get('src', '')
                    # except:
                    #     image = ''
                    result = self.get_image_src(topic)
                    image_data = {"image": result, "title": topic.get('title',""), "label": topic.get('label',"")}
                    image_data_list.append(image_data)

        else:
            pass
        logger.info(f"image_data_list: {image_data_list}")
        if len(image_data_list)>=1:
            image_data_filter = []
            for image_data in image_data_list:
                if image_data.get("image") != "":
                    image_data_filter.append(image_data)
            if len(image_data_filter)>0:
                for i in range(1,len(image_data_filter)+1):
                    _image_data=f"image_data{i}"
                    if image_data_filter[i-1]["image"] in self.image_data:
                        image_data_bytes=self.image_data[image_data_filter[i-1]["image"]]
                        dict_case[_image_data] = image_data_bytes
        logger.info(f"dict_case88888: {dict_case}")








    # def get_case_image2(self,dict_case, dict_data):
    #     # 检查是否有图片
    #     # if list(dict_data.keys()).__contains__("topics") and (dict_data['topics'][0].keys().__contains__("image") or dict_data['topics'][0]["topics"][0].keys().__contains__("image")):
    #
    #     # 判断是否存在键
    #     result = self.get_nested_value(dict_data, 'image')
    #     if result is not None:
    #         result = self.get_nested_value(dict_data, 'image')
    #     else:
    #         result = self.get_nested_value(dict_data, 'topics')
    #         if result is not None:
    #             result = False
    #             for item in dict_data["topics"]:
    #                 result = self.get_nested_value(item["topics"][0], 'image')
    #                 if result is not None:
    #                     result=True
    #                     break
    #                 else:
    #                     pass
    #             if result:
    #                 result2 = True
    #                 result = False
    #             else:
    #                 result2 = False
    #                 result= False
    #         else:
    #             result2 = False
    #             result = False
    #
    #
    #
    #
    #     if result:
    #     # if "topics" in dict_data.keys() and "image" in dict_data['topics'][0].keys():
    #         # 用例后第一层
    #         if dict_data['topics'][0].keys().__contains__("image"):
    #             _length = len(dict_data['topics'])
    #             if _length >= 1:
    #                  for i in range(1,_length+1):
    #                     # image = f"image{i}"
    #                     image_data=f"image_data{i}"
    #                     if "image" in dict_data['topics'][i-1]:
    #                         image_id = dict_data['topics'][i-1]["image"]
    #                         image_id = image_id["src"].split("/")[-1]
    #                         if self.image_data.get(image_id):
    #                             dict_case[image_data] = self.image_data[image_id]
    #                         else:
    #                             dict_case[image_data] = ""
    #                     else:
    #                         dict_case[image_data] = ""
    #             else:
    #                 pass
    #     # 用例后第二层
    #     # elif dict_data['topics'][0]["topics"][0].keys().__contains__("image"):
    #
    #     elif result2:
    #     # elif "image" in dict_data['topics'][0]["topics"][0].keys():
    #         _length = len(dict_data['topics'][0]["topics"])
    #         if _length >= 1:
    #             for i in range(1, _length + 1):
    #                 # image = f"image{i}"
    #                 image_data = f"image_data{i}"
    #                 if "image" in dict_data['topics'][0]["topics"][i - 1]:
    #                     image_id = dict_data['topics'][0]["topics"][i - 1]["image"]
    #                     image_id = image_id["src"].split("/")[-1]
    #                     if self.image_data.get(image_id):
    #                         dict_case[image_data] = self.image_data[image_id]
    #                     else:
    #                         dict_case[image_data] = ""
    #                 else:
    #                     dict_case[image_data] = ""
    #     # 第三层的直接抛弃不处理。
    #     else:
    #         return False
    #     # elif list(dict_data["topics"][0].keys()).__contains__("title"):
    #     #     labels = dict_data["topics"]['title']
    #     #     str_labels = ",".join(labels)
    #     #     dict_case['执行结果'] = str_labels
    #     # elif list(dict_data["topics"][0].keys()).__contains__("labels"):
    #     #     labels = dict_data["topics"]['labels']
    #     #     str_labels = ",".join(labels)
    #     #     dict_case['执行者'] = str_labels
    #     # else:
    #     #
    #     #     # dict_case['执行者'] = ""
    #     #     pass






    def check_max_module(self, module):
        """
        对最大模块数进行更新
        :param module:
        :return:
        """
        if module > self.maxModule:
            self.maxModule = module

    def case_format(self, dict_case, case):
        # print(111,dict_case)
        # print(222,case)
        """
        处理case，将标题，前置，步骤，预期等解析出来并添加到case中
        :param dict_case:
        :param case:
        :return:
        """

        if "：" in case or ":" in case:
            replace_case = case.replace("：", ":")
            if "前置:" in replace_case:
                indexPreconditions = replace_case.index("前置:")
                indexTestStep = replace_case.index("步骤:")
                indexExpected_Result = ""

                try:
                    if replace_case.find("期望:") != -1:
                        indexExpected_Result = replace_case.index("期望:")
                    elif replace_case.find("预期:") != -1:
                        indexExpected_Result = replace_case.index("预期:")
                    else:
                        raise ValueError

                except ValueError:
                    logger.info(f'未在用例中找到预期和期望,转换失败，退出程序，请检查该用例: \n{replace_case}')
                    exit()

                title = replace_case[3:indexPreconditions]
                Preconditions = replace_case[indexPreconditions + 3:indexTestStep]
                TestStep = replace_case[indexTestStep + 3:indexExpected_Result]
                ExpectedResult = replace_case[indexExpected_Result + 3:]
                dict_case["title"] = title.rstrip()
                dict_case["preconditions"] = Preconditions.rstrip()
                dict_case["TestStep"] = TestStep.rstrip()
                dict_case["ExpectedResult"] = ExpectedResult.rstrip()
            else:
                indexTestStep = replace_case.index("步骤:")
                indexPreconditions = ""
                try:
                    if replace_case.find("前置:") != -1:
                        indexPreconditions = replace_case.index("前置:")
                    elif replace_case.find("前置条件:") != -1:
                        indexPreconditions = replace_case.index("前置条件:")
                    else:
                        raise ValueError
                except ValueError:
                    logger.info(f'未在用例中找到前置或前置条件,转换失败，退出程序，请检查该用例: \n{replace_case}')
                    exit()

                indexExpected_Result = ""
                try:
                    if replace_case.find("期望:") != -1:
                        indexExpected_Result = replace_case.index("期望:")
                    elif replace_case.find("预期:") != -1:
                        indexExpected_Result = replace_case.index("预期:")
                    elif replace_case.find("预期结果:") != -1:
                        indexExpected_Result = replace_case.index("预期结果:")
                    else:
                        raise ValueError
                except ValueError:
                    logger.info(f'未在用例中找到预期和期望,转换失败，退出程序，请检查该用例: \n{replace_case}')
                    exit()
                Preconditions = replace_case[indexPreconditions + 3:indexTestStep]

                title = replace_case[3:indexTestStep]
                TestStep = replace_case[indexTestStep + 3:indexExpected_Result]
                ExpectedResult = replace_case[indexExpected_Result + 3:]
                dict_case["title"] = title.rstrip()
                dict_case["preconditions"] = Preconditions
                dict_case["TestStep"] = TestStep.rstrip()
                dict_case["ExpectedResult"] = ExpectedResult.rstrip()

if __name__ == '__main__':
    # file = "./data/【PRD 43.14】.xmind"
    file = "./data/【PRD 43.14】_副本.xmind"
    xmind=HandleXmind(xmind_file=file)
    xmind.handle_xmind()
    case_list=json.dumps(xmind.case_lists)
    logger.info(f"case_list: {case_list}")
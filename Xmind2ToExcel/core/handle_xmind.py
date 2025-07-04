# -*- ecoding: utf-8 -*-
# @ModuleName: handel_xmind
# @Author: Rex
# @Time: 2022/12/8 6:54 下午
import xmindparser
import json
from logger import logger as logger



class HandleXmind():
    def __init__(self, xmind_file):
        self.xmind_file = xmind_file
        self.firstSheetName = None
        self.sheetNames = []
        self.case_list = []
        self.case_lists = []
        self.maxModule = 0
    def __str__(self, *args, **kwargs):
        logger.info(f'用例数据解析完成 总计模块：{len(self.case_lists)}')
        for case in self.case_lists:
            logger.info(f'模块：{case["title"]} 用例数量：{len(case["Case"])}')
            logger.info(f'用例：{case}.\n')

    #
    # def handle_xmind(self):
    #     """
    #     通过xmindparser库将xmind中的数据转换成dict类型，拿到第一个节后后的所有数据
    #     [{'title': '一级模块', 'makers': ['priority-1'], 'topics': [{'title': '二级模块', 'makers': ['priority-2'],
    #     'topics': [{'title': '标题：demo-一级模块测试\n前置：前置可有可无\n步骤：进入一级模块\n预期：正常进入一级模块', 'makers': ['task-done'],
    #     'topics': [{'title': '标题：demo-二级模块测试-01\n前置：前置可有可无\n步骤：进入二级模块\n预期：正常进入二级模块', 'note': '进入二级模块失败',
    #     'makers': ['symbol-attention']}, {'title': '标题：demo-二级模块测试-02\n前置：前置可有可无\n步骤：进入二级模块\n预期：正常进入二级模块',
    #     'makers': ['symbol-attention'], 'labels': ['进入二级模块失败']}]}]}]}, {'title': '一级模块', 'makers': ['priority-1'],
    #     'topics': [{'title': '二级模块', 'makers': ['priority-2'], 'topics': [{'title': '三级模块', 'makers': ['priority-3'],
    #     'topics': [{'title': '标题：这是一个没有前置步骤的测试用例\n步骤：进入一级模块\n预期：正常进入一级模块', 'makers': ['task-done']},
    #      {'title': '没有标记 & 没有标题（不是Case）的节点将会被过滤，可以再这里写点逻辑性的内容，整理思路',
    #      'topics': [{'title': '标题：这里才是真正的Case\n步骤：进入三级模块\n预期：成功', 'makers': ['task-done']}]}]}]}]}]
    #     :return:
    #     """
    #     dict_data = xmindparser.xmind_to_dict(self.xmind_file)
    #     all_data = dict_data[0]['topic']
    #     # xmind内容主题，可用该名字作为最后Excel报告的文件名
    #     self.sheetName = all_data['title']
    #     # 获取所有的1级节点
    #     topics = all_data['topics']

    #     self.get_all_topic_data(topics, {})

    def handle_xmind(self):
        """
        通过xmindparser库将xmind中的数据转换成dict类型，拿到第一个节后后的所有数据
        [{'title': '一级模块', 'makers': ['priority-1'], 'topics': [{'title': '二级模块', 'makers': ['priority-2'],
        'topics': [{'title': '标题：demo-一级模块测试\n前置：前置可有可无\n步骤：进入一级模块\n预期：正常进入一级模块', 'makers': ['task-done'],
        'topics': [{'title': '标题：demo-二级模块测试-01\n前置：前置可有可无\n步骤：进入二级模块\n预期：正常进入二级模块', 'note': '进入二级模块失败',
        'makers': ['symbol-attention']}, {'title': '标题：demo-二级模块测试-02\n前置：前置可有可无\n步骤：进入二级模块\n预期：正常进入二级模块',
        'makers': ['symbol-attention'], 'labels': ['进入二级模块失败']}]}]}]}, {'title': '一级模块', 'makers': ['priority-1'],
        'topics': [{'title': '二级模块', 'makers': ['priority-2'], 'topics': [{'title': '三级模块', 'makers': ['priority-3'],
        'topics': [{'title': '标题：这是一个没有前置步骤的测试用例\n步骤：进入一级模块\n预期：正常进入一级模块', 'makers': ['task-done']},
         {'title': '没有标记 & 没有标题（不是Case）的节点将会被过滤，可以再这里写点逻辑性的内容，整理思路',
         'topics': [{'title': '标题：这里才是真正的Case\n步骤：进入三级模块\n预期：成功', 'makers': ['task-done']}]}]}]}]}]
        :return:
        """
        dict_data = xmindparser.xmind_to_dict(self.xmind_file)
        # print(dict_data)
        # all_data = dict_data[0]['topic']

        # print("6666",dict_data,'\n')
        #
        # all_data_test = dict_data[0]['topic']
        # print("6565",all_data_test)

        # logger.info(f'Xmind解析数据：{dict_data}')

        all_data = []
        for i in dict_data:
            # print(888,i)
            # title = i["title"]
            topic = i['topic']
            title = topic['title']

            topics = topic['topics']
            # 将数据组装放入列表
            dict = {"title":title, "topics": topics}
            # dict = {"title":title, "topics": topics}
            all_data.append(dict)

        # xmind内容主题，取第一个主题，可用该名字作为最后Excel报告的文件名
        self.firstSheetName = all_data[0]['title']

        # 每个画布的xmind主题内容,用该名字创建sheet
        for i in all_data:
            self.sheetNames.append(i['title'])

        # 获取所有的1级节点
        # topics = all_data['topics']

        # self.get_all_topic_data(topics, {})

        # 传入所有一级主题和数据
        # self.get_all_topic_data(all_data, {})

        for data in all_data:
            # print(len(self.case_list))
            new_case_list = []

            self.get_all_topic_data(data.get('title'), data.get('topics'),{})


            for case in self.case_list:
                new_dice_case={"module-1": data.get('title')}
                new_dice_case.update(case)
                # print(data.get("title"),"新CASE：",new_dice_case,'\n')
                new_case_list.append(new_dice_case)
            self.case_list = new_case_list

            dict = {"title":data.get('title'), "Case":self.case_list}
            self.case_list=[]
            self.case_lists.append(dict)
        # 打印所有用例数据
        logger.info(self.__str__())





    def get_all_topic_data(self, title, data, dic):
        """
        拿到所有topic的数据，并传递有效的case值 type dict
        :param data: 需要处理的数据
        :param dic: 有效的case type:dict
        :return:
        """
        dict_case=dic
        # print("传入：",data)
        # print("data长度",len(data))
        # 说明没有向下的分节点了
        if len(data) == 1:
            dict_data = data[0]
            # dict_data = data
            # 包含标签说明是我们想要的节点,并从该节点中提取title的值
            self.get_title_data(title,dict_case, dict_data)

        # 说明有向下的节点
        else:
            for i in range(len(data)):
                    # print(555999,data[i])
                if i == 0:
                    # 为了不改变第一次传进来的数据所以先copy一份用copy的数据进行组装
                    self.get_title_data(title,dic.copy(), data[i])
                else:
                    # 如果第一级节点包含了标签说明是我们想要的节点
                    if list(data[i].keys()).__contains__("makers"):
                        # 如果该节点包含 1 标记说明新的1级模块，此时不需要复制之前的case
                        if data[i]['makers'] == ['priority-1']:
                            new_dict_case = {}
                            self.get_title_data(title,new_dict_case, data[i])
                        # 如果该节点包含标签但不是 1，比如说 2，3，4 说明之前已经有出现过1级标签了，此时需要复制之前的case
                        else:
                            new_dict_case = dict_case.copy()
                            self.get_title_data(title,new_dict_case, data[i])
                    # 如果是case节点也是我们想要的
                    elif "标题" in data[i]['title'] and "步骤" in data[i]['title']:
                        new_dict_case = dict_case.copy()
                        self.get_title_data(title,new_dict_case, data[i])
                    # 如果该节点没有标签 且 标题也不在其中（不是case节点），则不是我们想要的节点，检查下是否有下级节点
                    else:
                        # 说明还有下一级节点,就直接递归
                        if list(data[i].keys()).__contains__("topics"):
                            new_dict_case = dict_case.copy()
                            self.get_all_topic_data(title,data[i]['topics'], new_dict_case)
                        # 如果没有下一级节点了，说明已经是最后的节点了，则把数据放到列表中

    def get_title_data(self,title, dict_case, dict_data):
        """
        处理数据，并拿到所有需要数据的titile值，并将数据填入有效的case值中（dict类型）（title对应的是用户在xmind中输入的数据）
        :param dict_case: 有效的case type dict
        :param dict_data: 需要处理的数据
        :return:
        """
        # print(999999,dict_data)
        # print(666666,dict_case.keys())

        if list(dict_case.keys()).__contains__("title"):
            # print(1212121, dict_case, '\n')
            # print(33333, dict_data, '\n')
            # logger.info(f'555555{dict_data["title"]}')
            # 先复制之前的case
            new_dict_case = dict_case.copy()

            if list(dict_data.keys()).__contains__("makers") and "标题" not in dict_data['title']:
                if dict_data['makers'] == ['priority-1']:
                    new_dict_case['module-1'] = dict_data['title']
                    self.check_max_module(1)
                elif dict_data['makers'] == ['priority-2']  or 'priority-2' in dict_data['makers']:
                    new_dict_case['module-2'] = dict_data['title']
                    self.check_max_module(2)
                elif dict_data['makers'] == ['priority-3'] or 'priority-3' in dict_data['makers']:
                    new_dict_case['module-3'] = dict_data['title']
                    self.check_max_module(3)
                elif dict_data['makers'] == ['priority-4'] or 'priority-4' in dict_data['makers']:
                    new_dict_case['module-4'] = dict_data['title']
                    self.check_max_module(4)
                elif dict_data['makers'] == ['priority-5']  or 'priority-5' in dict_data['makers']:
                    new_dict_case['module-5'] = dict_data['title']
                    self.check_max_module(5)


            # if "标题" in dict_data['title'] and "预期" in dict_data['title']:
            if "标题" in dict_data['title'] and "期望"or"预期" in dict_data['title']:
                case = dict_data['title']
                self.case_format(new_dict_case, case)
                # 设置case的状态
                self.set_case_status(new_dict_case, dict_data)
                # 提取case的note状态
                self.get_case_note_labels(new_dict_case, dict_data)
                self.case_list.append(new_dict_case)

            # 说明还有下一级节点,就直接递归
            if list(dict_data.keys()).__contains__("topics"):
                self.get_all_topic_data(title,dict_data['topics'], new_dict_case)
        else:
            if list(dict_data.keys()).__contains__("makers") and "标题" not in dict_data['title']:
                if dict_data['makers'] == ['priority-1']:
                    dict_case['module-1'] = dict_data['title']
                    self.check_max_module(1)

                elif dict_data['makers'] == ['priority-2'] or 'priority-2' in dict_data['makers']:
                    dict_case['module-2'] = dict_data['title']
                    self.check_max_module(2)

                elif dict_data['makers'] == ['priority-3'] or 'priority-3' in dict_data['makers']:
                    dict_case['module-3'] = dict_data['title']
                    self.check_max_module(3)

                elif dict_data['makers'] == ['priority-4'] or  'priority-4' in dict_data['makers']:
                    dict_case['module-4'] = dict_data['title']
                    self.check_max_module(4)

                elif dict_data['makers'] == ['priority-5']or  'priority-5' in dict_data['makers']:
                    dict_case['module-5'] = dict_data['title']
                    self.check_max_module(5)

            # logger.info(f'555555{dict_data["title"]}')

            # logger.info(f'未在用例中找到标题和预期,转换失败，退出程序，请检查该用例: \n{dict_data},\n{dict_case}')
            # 如果发现topics里面的内容只有一级包含标题的内容时 则需要检查case前面是否已经有了
            # 当出现标题在节点中时，该节点也不一定是最后一个节点，也许是case后面还有case
            try:
                if "标题" in dict_data['title'] and "期望" or"预期" in dict_data['title'] :
                # if "标题" in dict_data['title'] and "预期" in dict_data['title']:
                    # 如果case的字典里已经有case的字段了，说明这个是case后面的case
                    if list(dict_case.keys()).__contains__("title"):
                        # 先复制之前的case
                        new_dict_case = dict_case.copy()
                        # 再把case字段的值替换掉
                        case = dict_data['title']
                        self.case_format(new_dict_case, case)
                        # 设置case的状态
                        self.set_case_status(new_dict_case, dict_data)
                        # 提取case的note状态
                        self.get_case_note_labels(new_dict_case, dict_data)
                        self.case_list.append(new_dict_case)
                    else:
                        case = dict_data['title']
                        self.case_format(dict_case, case)
                        # 检查这个case中是否有makers标记并设置case的状态
                        self.set_case_status(dict_case, dict_data)
                        # 检查这个case中是否有note标记 有的话把note提取出来
                        self.get_case_note_labels(dict_case, dict_data)
                        # 先把前面的case加入到case列表中
                        self.case_list.append(dict_case)
            except:
                logger.error(f'未在用例中找到标题和预期,转换失败，退出程序，请检查该用例: {dict_case},{dict_data}')
                exit()


            # self.case_lists.append(self.case_list)
            # 说明还有下一级节点,就直接递归
            if list(dict_data.keys()).__contains__("topics"):
                self.get_all_topic_data(title,dict_data['topics'], dict_case)
            # 如果没有下一级节点了，说明已经是最后的节点了，则把数据放到列表中

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
                indexPreconditions=""
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
    xmind_file = "../Demo.xmind"
    # xmind_file = "../WebPortal总用例整理_v0.1 (1).xmind"
    # xmind_file = "../Webportal总用例 v3.1.0.xmind"
    # xmind_file = "../VV360FW测试case.xmind"
    # xmind_file = "../VV360FW功能测试case11111.xmind"
    xmind_handler = HandleXmind(xmind_file)
    xmind_handler.handle_xmind()


    # print("111",xmind_handler.case_list)
    # # print(len(xmind_handler.case_list))
    # for case in xmind_handler.case_list:
    #     print(case)
    # logger.info(f'用例数据：{xmind_handler.case_lists}')
    # for case in xmind_handler.case_lists:
    #     # print('\n',"CASE: ",case,'\n')
    #
    #     print(len(case['Case']))
    #     for case in case['Case']:
    #         print(case)


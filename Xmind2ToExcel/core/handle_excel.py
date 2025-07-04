# -*- ecoding: utf-8 -*-
# @ModuleName: handel_excel
# @Author: Rex
# @Time: 2023/1/15 8:15 下午

from logger import logger as logger
import xlwt
import re
class HandleExcel():
    def __init__(self,fileName,filePath):
        # fileName既作为文件名也作为sheet name
        self.fileName=fileName
        self.workbook=xlwt.Workbook(encoding='utr-8')
        # self.worksheet=self.workbook.add_sheet(fileName)
        # self.worksheet=self.workbook.add_sheet(sheetname=fileName)
        self.worksheet=None
        self.case_demo={}
        self.filePath=filePath

    def generate_title(self,maxModule,data_list):
        status=""
        data_list=data_list.copy()


        title_list=["Number"]
        # 根据模块的级数生成对应的标题
        for item in range(maxModule):
            module = "Module-"+str(item + 1)
            title_list.append(module)


        # 适配Webportal用例逻辑
        datalist_lenth=len(data_list)
        if datalist_lenth>1:
            lenth=1
        else:
            lenth=0
        # logger.info(f"重新转换后的数据：{data_list}")
        if maxModule == 5 and "PRD" in data_list[lenth]["Case"][0]["module-5"]:
            title_list = ["Number", "Version", "PRD", "Module-1", "Module-2", "Module-3"]
        elif maxModule == 6  and "PRD" in data_list[lenth]["Case"][0]["module-5"]:
            title_list = ["Number", "Version", "PRD", "Module-1", "Module-2", "Module-3", "Module-6"]
        else:
            pass


        if  "Version" in title_list and "PRD" in title_list:
            # 重组数据列名
            key_mapping = {"module-4": "version", "module-5": "prd"}  # 定义键名映射
            for data in data_list:
                case_list = data["Case"]
                new_case_list = []
                for case in case_list:
                    case = {key_mapping.get(k, k): v for k, v in case.items()}
                    new_case_list.append(case)
                data["Case"] = new_case_list
        else:
            pass

        title_list+=["Test Item","Preconditions","Test Step","Expected Result","Result","Regression","Note","Status"]


        # 获取title名称并创建sheet
        for data in data_list:
            sheetname = data['title']

            logger.info(f'sheetname:,{sheetname}')

            self.worksheet = self.workbook.add_sheet(sheetname)

            # 将生成的标题写入到excel中
            for i in range(len(title_list)):
                # 标题在第一行，所以行号都固定为0，列号对应标题列表的索引值

                self.worksheet.write(0, i, title_list[i])
                # 拿到所有标题的列号，并生成一个字典，存储每个标题及对应的列号

                if title_list[i] == "Test Item":
                    self.case_demo ['title'] = i
                elif title_list[i] == "Test Step":
                    self.case_demo['TestStep']=i
                elif title_list[i] == "Expected Result":
                    self.case_demo['ExpectedResult']=i
                elif title_list[i] == "Result":
                    self.case_demo['case_status']=i
                elif title_list[i] == "Regression":
                    self.case_demo['regression']=i
                elif title_list[i] == "Note":
                    self.case_demo['note']=i
                elif title_list[i] == "Status":
                    self.case_demo['status']=i
                else:
                    self.case_demo[title_list[i].lower()] = i
            # print(333,self.case_demo)
            case_list = data['Case']
            status=self.write_data(case_list)
            self.workbook.save(f"{self.filePath}.xls")
        return status

    # def write_data(self,data_list):
    #     for case in data_list:
    #         #写入数据到指定sheet页
    #         self.worksheet = self.workbook.add_sheet(case['title'])
    #         # 读取case列表
    #         data_list = case['Case']
    #         # print(1111,data_list)
    #         try:
    #             for item in range(len(data_list)):
    #                 case_id = item + 1
    #                 self.worksheet.write(case_id,0 ,case_id)
    #                 # 拿到所有case的key
    #                 key_list=list(data_list[item].keys())
    #                 for key in range(len(key_list)):
    #                     # 根据case的key 从 case_demo中拿到对应标题列号
    #                     # case的每个key都对应case_demo中的key
    #                     clo= self.case_demo[key_list[key]]
    #                     self.worksheet.write(case_id,clo ,data_list[item][key_list[key]])
    #             self.workbook.save(f"{self.filePath}.xls")
    #             return True
    #         except:
    #             return False
    def write_data(self,data_list):
        logger.info(f'用例数量：{len(data_list)}')
        # print(777,self.case_demo)
        try:
            for item in range(len(data_list)):

                case_id = item + 1

                # 拿到所有case的key
                key_list=list(data_list[item].keys())
                # print(999,key_list)
                # print(888,len(key_list))

                # print(key_list)
                try:
                    Note = data_list[item]['note']
                    if Note != None and Note != '':
                        # logger.info(f"测试：{Note}")
                        match = re.search(r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})', Note)
                        if match:
                            year, month, day, hour, minute, second = match.groups()
                            date_id = f'{year}{month}{day}{hour}{minute}{second}'
                            # 字符串数值转为int
                            unique_id = int(date_id)
                            # 设置列属性为数字,小数位数为0
                            style=xlwt.XFStyle()
                            style.num_format_str='0'

                            self.worksheet.write(case_id, 0, unique_id, style)
                        else:
                            self.worksheet.write(case_id, 0, case_id)
                    else:
                        self.worksheet.write(case_id, 0, case_id)
                except:
                    # logger.info(f"Note信息无字符串时间！！！")
                    continue


                for key in range(len(key_list)):
                    # print(666,key)
                    # 根据case的key 从 case_demo中拿到对应标题列号
                    # case的每个key都对应case_demo中的key
                    clo= self.case_demo[key_list[key]]
                    # print(222,key_list[key])

                    self.worksheet.write(case_id,clo ,data_list[item][key_list[key]])
                    # print(111,data_list[item])


            # self.workbook.save(f"{self.filePath}.xls")
            return True
        except:
            logger.error(f"请检查xmind用例目录下,excle文件是否被占用！")
            return False


if __name__ == '__main__':
    # data_list = [
    #     {'title': 'Demo1', 'Case': [
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-一级模块测试-1', 'preconditions': '前置可有可无',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS', 'regression': '1',
    #          'note': ''
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-二级模块测试-01', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '2',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-二级模块测试-02', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '3',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': '这是一个没有前置步骤的测试用例', 'preconditions': '',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS',
    #          'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': '这里才是真正的Case', 'preconditions': '', 'TestStep': '进入三级模块',
    #          'ExpectedResult': '成功', 'case_status': 'PASS', 'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-一级模块测试-1', 'preconditions': '前置可有可无',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS', 'regression': '1',
    #          'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-二级模块测试-01', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '2',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-二级模块测试-02', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '3',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': '这是一个没有前置步骤的测试用例', 'preconditions': '',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS',
    #          'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': '这里才是真正的Case', 'preconditions': '', 'TestStep': '进入三级模块',
    #          'ExpectedResult': '成功', 'case_status': 'PASS', 'regression': 'N/A', 'note': ''
    #          }
    #     ]
    #      },
    #     {'title': 'Demo2', 'Case': [
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-一级模块测试-1', 'preconditions': '前置可有可无',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS', 'regression': '1',
    #          'note': ''
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-二级模块测试-01', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '2',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': 'demo-二级模块测试-02', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '3',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': '这是一个没有前置步骤的测试用例', 'preconditions': '',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS',
    #          'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块', 'module-2': '二级模块', 'module-3': '三级模块', 'module-4': '四级模块版本',
    #          'module-5': '五级模块PRD', 'title': '这里才是真正的Case', 'preconditions': '', 'TestStep': '进入三级模块',
    #          'ExpectedResult': '成功', 'case_status': 'PASS', 'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-一级模块测试-1', 'preconditions': '前置可有可无',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS', 'regression': '1',
    #          'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-二级模块测试-01', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '2',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': 'demo-二级模块测试-02', 'preconditions': '前置可有可无',
    #          'TestStep': '进入二级模块', 'ExpectedResult': '正常进入二级模块', 'case_status': 'FAIL', 'regression': '3',
    #          'note': '进入二级模块失败'
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': '这是一个没有前置步骤的测试用例', 'preconditions': '',
    #          'TestStep': '进入一级模块', 'ExpectedResult': '正常进入一级模块', 'case_status': 'PASS',
    #          'regression': 'N/A', 'note': ''
    #          },
    #         {'module-1': '一级模块2', 'module-2': '二级模块2', 'module-3': '三级模块2', 'module-4': '四级模块版本2',
    #          'module-5': '五级模块PRD2', 'title': '这里才是真正的Case', 'preconditions': '', 'TestStep': '进入三级模块',
    #          'ExpectedResult': '成功', 'case_status': 'PASS', 'regression': 'N/A', 'note': ''
    #          }
    #     ]
    #      }
    # ]
    data_list=[{'title': 'Dashboard', 'Case': [{'module-1': 'Dashboard', 'module-2': 'Events Dashboard', 'module-3': 'View', 'module-4': 'v3.0.0', 'module-5': '【PRD 40.1】', 'title': 'Fall Detection的Category的字段检查', 'preconditions': '\n1. 当前已登录\n2. RPM Alert Dashboard中存在Fall Detection', 'TestStep': '\n1. 查看Fall Detection的Category字段的值', 'ExpectedResult': '\n1. 值为Fall Detection', 'case_status': 'N/A', 'note': ''}, {'module-1': 'Dashboard', 'module-2': 'Events Dashboard', 'module-3': 'View', 'module-4': 'v3.0.0', 'module-5': '【PRD 40.1】', 'title': 'Fall Detection的Category的字段检查', 'preconditions': '\n1. 当前已登录\n2. RPM Alert Dashboard中存在Fall Detection', 'TestStep': '\n1. 查看Fall Detection的Alert字段的值', 'ExpectedResult': '\n1. 值为[Alert Color Flag] +\r Patient Falls + Icon且正确展示', 'case_status': 'N/A', 'note': ''}, {'module-1': 'Dashboard', 'module-2': 'Events Dashboard', 'module-3': 'View', 'module-4': 'v3.0.0', 'module-5': '【PRD 40.1】', 'title': 'Fall Detection的所有字段检查', 'preconditions': '\n1. 当前已登录\n2. RPM Alert Dashboard中存在Fall Detection', 'TestStep': '\n1. 查看生成的Fall Detection的所有字段的值', 'ExpectedResult': '\n1. Patient ID至Device ID等对应的值均正确展示', 'case_status': 'N/A', 'note': ''}]}, {'title': 'RT-RPM', 'Case': [{'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“physician”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面', 'TestStep': '\n1.查看查询默认文案\n2.下拉框选择一个或者多个“group”，点击查询\n3.下拉框选择“group”，且组合其他筛选项，点击查询', 'ExpectedResult': '\n1.查询输入框默认文案为“Group”\n2.查询结果为对应“group”的“patient”\n3.查询结果为组合条件的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“site”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”已选中“Enable Sites”', 'TestStep': '\n1.查看查询默认文案\n2.下拉框选择一个“site”或者多个“site”，点击查询\n3.下拉框选择“site”，且组合其他筛选项，点击查询', 'ExpectedResult': '\n1.查询输入框默认文案为“Site”\n2.查询结果为对应“site”的“patient”\n3.查询结果为组合条件的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“study”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”已选中“Enable Study”', 'TestStep': '\n1.查看查询默认文案\n2.下拉框选择一个“study”或者多个“study”，点击查询\n3.下拉框选择“study”，且组合其他筛选项，点击查询', 'ExpectedResult': '\n1.查询输入框默认文案为“Study”\n2.查询结果为对应“study”的“patient”\n3.查询结果为组合条件的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“subject ID”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”已勾选“Use Subject ID instead of Patient Name”', 'TestStep': '\n1.查看查询默认文案与列表字段\n2.输入正确的“subject ID”，点击查询\n3.输入部分“subject ID”，点击查询\n4.输入错误的“subject ID”、或者空格、或者特殊字符，点击查询\n5.输入大小写不一致的“subject ID”，点击查询', 'ExpectedResult': '\n1.查询输入框默认文案为“Subject ID”，列表字段为“subject ID”\n2.查询结果为对应的“patient”\n3.查询结果包含模糊匹配的“patient”\n4.查询结果为空，返回“No  Data”\n5.查询结果为忽略大小写后对应的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“subject ID”与其他筛选项组合查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”已勾选“Use Subject ID instead of Patient Name”', 'TestStep': '\n1.输入正确的“subject ID”，且下拉框选择“site”，点击查询\n2.输入正确的“subject ID”，且下拉框选择“study”，点击查询\n3.输入正确的“subject ID”，且下拉框选择“group”，点击查询\n4.输入正确的“subject ID”，且任意选择其他多个筛选项，点击查询', 'ExpectedResult': '\n1.查询结果为对应“subject ID”与“site”组合的“patient”\n2.查询结果为对应“subject ID”与“study”组合的“patient”\n3.查询结果为对应“subject ID”与“group”组合的“patient”\n4.查询结果为对应“subject ID”与其他筛选条件组合的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“patient name”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”未勾选“Use Subject ID instead of Patient Name”', 'TestStep': '\n1.查看查询默认文案与列表字段\n2.输入正确的“patient name”，点击查询\n3.输入部分“patient name”，点击查询\n4.输入错误的“patient name”、或者空格、或者特殊字符，点击查询\n5.输入大小写不一致的“patient name”，点击查询', 'ExpectedResult': '\n1.查询输入框默认文案为“patient name”，列表字段为“First Name, Last Name”\n2.查询结果为对应的“patient”\n3.查询结果包含模糊匹配的“patient”\n4.查询结果为空，返回“No  Data”\n5.查询结果为忽略大小写后对应的“patient”', 'case_status': 'N/A', 'note': ''}, {'module-1': 'RT-RPM', 'module-2': 'Multi Patient-Grid View', 'module-3': 'Search', 'module-4': 'v3.0.0', 'module-5': '【PRD RT-RPM】', 'title': '检查“RT-RPM”页面中按照“patient name”查询功能', 'preconditions': '\n1.当前账号有权限进入“RT-RPM”页面\n2.当前“clinic”未勾选“Use Subject ID instead of Patient Name”', 'TestStep': '\n1.输入正确的“patient name”，且下拉框选择“site”，点击查询\n2.输入正确的“patient name”，且下拉框选择“study”，点击查询\n3.输入正确的“patient name”，且下拉框选择“group”，点击查询\n4.输入正确的“patient name”，且任意选择其他多个筛选项，点击查询', 'ExpectedResult': '\n1.查询结果为对应“patient name”与“site”组合的“patient”\n2.查询结果为对应“patient name”与“study”组合的“patient”\n3.查询结果为对应“patient name”与“group”组合的“patient”\n4.查询结果为对应“patient name”与其他筛选条件组合的“patient”', 'case_status': 'N/A', 'note': ''}]}]
    excelHandle = HandleExcel("test2",'test2')
    excelHandle.generate_title(5,data_list)
    # excelHandle.write_data(data_list)





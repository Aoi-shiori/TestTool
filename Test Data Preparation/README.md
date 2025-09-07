代码结构说明
这个重构后的测试数据准备系统按照您的要求，将代码分为三个文件：

1. webportal_client.py
包含所有与WebPortal系统交互的函数

实现了认证、病人和设备操作、绑定和解绑等功能

提供了完整的WebPortal API封装

2. vcloud_client.py
包含所有与Vcloud系统交互的函数

实现了认证、病人注册、设备绑定等功能

提供了完整的Vcloud API封装

3. test_data_manager.py (主文件)
包含业务逻辑和功能实现

使用WebPortalClient和VCloudClient来执行具体操作

实现了设备创建、病人创建、绑定、解绑、删除、查询等功能

功能分类
功能一：基础数据创建
create_devices(): 创建设备并保存到Excel

create_patients(): 创建病人并保存到Excel

create_both(): 同时创建病人和设备

功能二：病人和设备绑定
bind_devices(): 读取Excel数据进行绑定

quick_patient_assignment(): 快速病人分配功能

自动保存绑定信息到patient_devices.json

功能三：病人和设备解绑
unbind_devices(): 读取Excel数据进行解绑

功能四：病人和设备删除
delete_resources(): 读取Excel数据进行删除

功能五：病人绑定信息
query_binding_info(): 查询绑定信息并保存到JSON文件

使用方式
创建TestDataManager实例

准备配置参数

调用execute_operation方法执行指定操作

python
# 创建管理器
manager = TestDataManager()

# 准备配置
config = {
    "device_rules": [
        {"type": "ECG", "pattern": "ECGRec_202509/JD00{index}"},
        {"type": "BP", "pattern": "BP5C_J20250906{index}"}
    ],
    "count": 5
}

# 执行操作
manager.execute_operation(OperationMode.CREATE_DEVICES, config)
这个重构后的系统提供了更清晰的代码结构，更好的模块化分离，以及更灵活的使用方式。每个客户端都专注于自己的API调用，而主文件则负责业务逻辑和协调不同客户端之间的操作。
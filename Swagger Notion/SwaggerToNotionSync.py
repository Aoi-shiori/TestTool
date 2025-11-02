import yaml
import json
import requests
from notion_client import Client
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import re
import time
import logging
from requests.auth import HTTPBasicAuth

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(thread)d %(filename)s[line:%(lineno)d]->%(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('swagger_to_notion.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ParameterInfo:
    name: str
    param_type: str
    description: str
    required: bool
    example: Any = None
    enum: List[Any] = None
    default: Any = None


@dataclass
class ResponseInfo:
    status_code: str
    description: str
    schema_ref: str = None
    example: Any = None


@dataclass
class EndpointInfo:
    path: str
    method: str
    summary: str
    description: str
    tags: List[str]
    parameters: List[ParameterInfo]
    responses: List[ResponseInfo]
    security: List[Dict] = None
    consumes: List[str] = None
    produces: List[str] = None


@dataclass
class SchemaProperty:
    name: str
    prop_type: str
    description: str
    example: Any = None
    enum: List[Any] = None
    required: bool = False
    default: Any = None


@dataclass
class SchemaDefinition:
    name: str
    description: str
    properties: List[SchemaProperty]
    example: Any = None


class SwaggerToNotionSync:
    def __init__(self, notion_token: str, parent_page_id: str = None, database_name: str = "API 接口文档"):
        self.notion = Client(auth=notion_token)
        # 规范化父页面ID格式（移除连字符）
        self.parent_page_id = self.normalize_page_id(parent_page_id) if parent_page_id else None
        self.database_name = database_name
        self.database_id = None
        self.swagger_data = None
        self.endpoints = []
        self.schemas = {}
        self.security_definitions = {}
        self.existing_pages_cache = {}
        self.database_properties = {}
        self.current_page_id = None
        self.batch_size = 50
        self.delay_between_requests = 0.05

        # 立即尝试查找或创建数据库
        self.ensure_database_exists()

    def normalize_page_id(self, page_id: str) -> str:
        """规范化页面ID格式，移除连字符"""
        # 移除所有连字符，转换为纯数字和字母
        normalized = re.sub(r'[^a-zA-Z0-9]', '', page_id)
        logger.debug(f"规范化页面ID: {page_id} -> {normalized}")
        return normalized

    def find_existing_database(self) -> Optional[str]:
        """根据名称查找已存在的数据库 - 修复ID格式问题"""
        logger.info(f"正在查找数据库: {self.database_name}")
        try:
            # 搜索所有数据库
            response = self.notion.search(
                query=self.database_name,
                filter={"property": "object", "value": "database"}
            )

            for db in response.get('results', []):
                # 检查数据库标题
                title = db.get('title', [])
                db_title = title[0].get('text', {}).get('content') if title else None

                if db_title == self.database_name:
                    # 获取父页面信息
                    parent = db.get('parent', {})
                    parent_type = parent.get('type')

                    # 检查父页面是否匹配
                    if parent_type == 'page_id':
                        parent_page_id = parent.get('page_id', '')
                        # 规范化父页面ID进行比较
                        normalized_parent_id = self.normalize_page_id(parent_page_id)

                        if normalized_parent_id == self.parent_page_id:
                            logger.info(f"✅ 找到现有数据库: {db['id']}")
                            return db['id']

                    elif parent_type == 'workspace' and not self.parent_page_id:
                        # 如果数据库在workspace根目录，且没有指定parent_page_id
                        logger.info(f"✅ 找到现有数据库(workspace): {db['id']}")
                        return db['id']

            logger.info("未找到现有数据库，将创建新数据库")
            return None

        except Exception as e:
            logger.warning(f"查找数据库失败: {e}")
            return None

    def create_database(self) -> str:
        """自动创建 Notion 数据库"""
        logger.info(f"正在创建 Notion 数据库: {self.database_name}")

        properties = {
            "接口名称": {"title": {}},
            "接口路径": {"rich_text": {}},
            "HTTP方法": {
                "select": {
                    "options": [
                        {"name": "GET", "color": "blue"},
                        {"name": "POST", "color": "green"},
                        {"name": "PUT", "color": "orange"},
                        {"name": "DELETE", "color": "red"},
                        {"name": "PATCH", "color": "purple"},
                        {"name": "HEAD", "color": "default"},
                        {"name": "OPTIONS", "color": "default"}
                    ]
                }
            },
            "标签": {"multi_select": {}},
            "状态": {"status": {}},
            "最后同步时间": {"date": {}},
            "分类": {"select": {}},
            "安全性": {"select": {}},
            "请求格式": {"multi_select": {}},
            "响应格式": {"multi_select": {}},
            "描述": {"rich_text": {}},
            "参数数量": {"number": {}},
            "是否需要认证": {"checkbox": {}}
        }

        try:
            # 使用原始的带连字符的父页面ID创建数据库
            original_parent_id = None
            if self.parent_page_id:
                # 尝试恢复原始的带连字符格式（如果可能）
                if len(self.parent_page_id) == 32:  # 无连字符的UUID长度
                    # 尝试重新插入连字符：8-4-4-4-12
                    try:
                        original_parent_id = f"{self.parent_page_id[:8]}-{self.parent_page_id[8:12]}-{self.parent_page_id[12:16]}-{self.parent_page_id[16:20]}-{self.parent_page_id[20:]}"
                        logger.debug(f"尝试恢复父页面ID格式: {self.parent_page_id} -> {original_parent_id}")
                    except:
                        original_parent_id = self.parent_page_id
                else:
                    original_parent_id = self.parent_page_id

                database = self.notion.databases.create(
                    parent={"page_id": original_parent_id},
                    title=[{"type": "text", "text": {"content": self.database_name}}],
                    properties=properties
                )
            else:
                database = self.notion.databases.create(
                    title=[{"type": "text", "text": {"content": self.database_name}}],
                    properties=properties
                )

            self.database_id = database['id']
            logger.info(f"✅ 基础数据库创建成功: {self.database_id}")

            time.sleep(1)
            self.update_status_options()
            self.verify_database_properties()

            logger.info("📊 数据库字段已自动创建")
            return self.database_id

        except Exception as e:
            logger.error(f"❌ 数据库创建失败: {str(e)}")
            raise

    def update_status_options(self):
        """更新状态字段的选项"""
        try:
            status_options = [
                {"name": "✅ 活跃", "color": "green"},
                {"name": "🔄 维护中", "color": "yellow"},
                {"name": "❌ 废弃", "color": "red"},
                {"name": "📝 待测试", "color": "blue"},
                {"name": "⏳ 开发中", "color": "orange"}
            ]

            self.notion.databases.update(
                database_id=self.database_id,
                properties={
                    "状态": {
                        "status": {
                            "options": status_options
                        }
                    }
                }
            )
            logger.info("✅ 状态字段选项更新成功")
        except Exception as e:
            logger.warning(f"⚠️ 状态字段选项更新失败: {e}")

    def verify_database_properties(self):
        """验证数据库属性是否存在"""
        try:
            database = self.notion.databases.retrieve(database_id=self.database_id)
            self.database_properties = database.get('properties', {})

            required_properties = ["接口名称", "接口路径", "HTTP方法", "状态"]
            missing_properties = [prop for prop in required_properties if prop not in self.database_properties]

            if missing_properties:
                logger.warning(f"⚠️ 缺少以下属性: {', '.join(missing_properties)}")
                self.create_missing_properties(missing_properties)
            else:
                logger.info("✅ 所有必需属性验证通过")

        except Exception as e:
            logger.warning(f"⚠️ 验证数据库属性失败: {e}")

    def create_missing_properties(self, missing_properties):
        """创建缺失的属性"""
        property_definitions = {
            "接口名称": {"title": {}},
            "接口路径": {"rich_text": {}},
            "HTTP方法": {"select": {"options": []}},
            "状态": {"status": {}},
            "标签": {"multi_select": {}},
            "最后同步时间": {"date": {}},
            "分类": {"select": {}},
            "安全性": {"select": {}},
            "请求格式": {"multi_select": {}},
            "响应格式": {"multi_select": {}},
            "描述": {"rich_text": {}},
            "参数数量": {"number": {}},
            "是否需要认证": {"checkbox": {}}
        }

        update_properties = {}
        for prop in missing_properties:
            if prop in property_definitions:
                update_properties[prop] = property_definitions[prop]

        if update_properties:
            try:
                self.notion.databases.update(
                    database_id=self.database_id,
                    properties=update_properties
                )
                logger.info("✅ 缺失属性创建成功")
            except Exception as e:
                logger.error(f"❌ 创建缺失属性失败: {e}")

    def ensure_database_exists(self):
        """确保数据库存在，优先使用现有数据库"""
        if not self.database_id:
            # 先尝试查找现有数据库
            self.database_id = self.find_existing_database()

            if not self.database_id:
                # 如果没找到，创建新数据库
                self.database_id = self.create_database()
            else:
                # 如果找到了现有数据库，验证属性
                self.verify_database_properties()

    def load_swagger_file(self, file_path: str):
        """加载并解析 Swagger YAML 文件"""
        logger.info(f"正在加载 Swagger 文件: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                self.swagger_data = yaml.safe_load(file)

            if 'securityDefinitions' in self.swagger_data:
                self.security_definitions = self.swagger_data['securityDefinitions']

            if 'definitions' in self.swagger_data:
                self.parse_definitions()

            self.parse_paths()

            logger.info(f"✅ 解析完成: {len(self.endpoints)} 个接口, {len(self.schemas)} 个数据模型")

        except Exception as e:
            logger.error(f"❌ 加载 Swagger 文件失败: {str(e)}")
            raise

    def parse_definitions(self):
        """解析数据模型定义"""
        for schema_name, schema_info in self.swagger_data['definitions'].items():
            properties = []

            if 'properties' in schema_info:
                required_fields = schema_info.get('required', [])

                for prop_name, prop_info in schema_info['properties'].items():
                    prop = SchemaProperty(
                        name=prop_name,
                        prop_type=prop_info.get('type', 'object'),
                        description=prop_info.get('description', ''),
                        example=prop_info.get('example'),
                        enum=prop_info.get('enum'),
                        required=prop_name in required_fields,
                        default=prop_info.get('default')
                    )
                    properties.append(prop)

            schema = SchemaDefinition(
                name=schema_name,
                description=schema_info.get('description', ''),
                properties=properties,
                example=schema_info.get('example')
            )

            self.schemas[schema_name] = schema

    def parse_paths(self):
        """解析所有接口路径"""
        for path, methods in self.swagger_data['paths'].items():
            for method, method_info in methods.items():
                if method.lower() not in ['get', 'post', 'put', 'delete', 'patch', 'head', 'options']:
                    continue

                parameters = []
                if 'parameters' in method_info:
                    parameters = self.parse_parameters(method_info['parameters'])

                if 'requestBody' in method_info or any(
                        p.get('in') == 'body' for p in method_info.get('parameters', [])):
                    body_params = self.parse_request_body(method_info)
                    parameters.extend(body_params)

                responses = self.parse_responses(method_info.get('responses', {}))

                security = method_info.get('security', [])
                consumes = method_info.get('consumes', [])
                produces = method_info.get('produces', [])

                endpoint = EndpointInfo(
                    path=path,
                    method=method.upper(),
                    summary=method_info.get('summary', ''),
                    description=method_info.get('description', ''),
                    tags=method_info.get('tags', []),
                    parameters=parameters,
                    responses=responses,
                    security=security,
                    consumes=consumes,
                    produces=produces
                )

                self.endpoints.append(endpoint)

    def parse_parameters(self, parameters: List[Dict]) -> List[ParameterInfo]:
        """解析接口参数"""
        result = []
        for param in parameters:
            param_type = param.get('in', 'query')
            example = param.get('example')
            default = param.get('default')

            if 'schema' in param:
                schema = param['schema']
                if 'type' in schema:
                    param_type = schema.get('type', param_type)
                if 'example' in schema and example is None:
                    example = schema.get('example')
                if 'default' in schema and default is None:
                    default = schema.get('default')

            param_info = ParameterInfo(
                name=param['name'],
                param_type=param_type,
                description=param.get('description', ''),
                required=param.get('required', False),
                example=example,
                enum=param.get('enum'),
                default=default
            )

            result.append(param_info)

        return result

    def parse_request_body(self, method_info: Dict) -> List[ParameterInfo]:
        """解析请求体参数"""
        result = []

        for param in method_info.get('parameters', []):
            if param.get('in') == 'body' and 'schema' in param:
                schema_ref = param['schema'].get('$ref', '')
                if schema_ref:
                    schema_name = schema_ref.split('/')[-1]
                    if schema_name in self.schemas:
                        for prop in self.schemas[schema_name].properties:
                            body_param = ParameterInfo(
                                name=prop.name,
                                param_type='body',
                                description=prop.description,
                                required=prop.required,
                                example=prop.example,
                                enum=prop.enum,
                                default=prop.default
                            )
                            result.append(body_param)

        if 'requestBody' in method_info:
            content = method_info['requestBody'].get('content', {})
            for content_type, content_info in content.items():
                if 'schema' in content_info:
                    schema_ref = content_info['schema'].get('$ref', '')
                    if schema_ref:
                        schema_name = schema_ref.split('/')[-1]
                        if schema_name in self.schemas:
                            for prop in self.schemas[schema_name].properties:
                                body_param = ParameterInfo(
                                    name=prop.name,
                                    param_type='body',
                                    description=prop.description,
                                    required=prop.required,
                                    example=prop.example,
                                    enum=prop.enum,
                                    default=prop.default
                                )
                                result.append(body_param)

        return result

    def parse_responses(self, responses: Dict) -> List[ResponseInfo]:
        """解析响应信息"""
        result = []
        for status_code, response_info in responses.items():
            response = ResponseInfo(
                status_code=status_code,
                description=response_info.get('description', ''),
            )

            if 'schema' in response_info:
                schema_ref = response_info['schema'].get('$ref', '')
                if schema_ref:
                    response.schema_ref = schema_ref.split('/')[-1]

            if 'examples' in response_info:
                for example_name, example_value in response_info['examples'].items():
                    if 'value' in example_value:
                        response.example = example_value['value']
                        break
            elif 'example' in response_info:
                response.example = response_info['example']

            result.append(response)

        return result

    def get_existing_pages(self):
        """获取数据库中已存在的页面 - 改进的判断逻辑"""
        logger.info("正在获取数据库中已存在的页面...")
        try:
            results = self.notion.databases.query(database_id=self.database_id)
            logger.info(f"查询到 {len(results['results'])} 个页面")

            for page in results['results']:
                properties = page['properties']

                # 获取接口路径
                path_prop = properties.get('接口路径', {}).get('rich_text', [])
                path = path_prop[0]['text']['content'] if path_prop else None

                # 获取HTTP方法
                method_prop = properties.get('HTTP方法', {}).get('select', {})
                method = method_prop.get('name') if method_prop else None

                # 获取接口名称作为备用标识
                name_prop = properties.get('接口名称', {}).get('title', [])
                name = name_prop[0]['text']['content'] if name_prop else None

                # 使用多种方式生成唯一标识
                if path and method:
                    # 主要标识：方法 + 路径
                    key1 = f"{method} {path}"
                    self.existing_pages_cache[key1] = page['id']
                    logger.debug(f"找到页面: {key1} -> {page['id']}")

                if name:
                    # 备用标识：接口名称
                    key2 = f"name:{name}"
                    self.existing_pages_cache[key2] = page['id']
                    logger.debug(f"找到页面(名称): {key2} -> {page['id']}")

                # 直接使用页面ID作为键
                self.existing_pages_cache[page['id']] = page['id']

            logger.info(f"找到 {len(self.existing_pages_cache)} 个已存在的页面标识")

        except Exception as e:
            logger.warning(f"⚠️ 获取现有页面失败: {e}")
            self.existing_pages_cache = {}

    def find_existing_page_id(self, endpoint: EndpointInfo) -> Optional[str]:
        """查找已存在页面的ID - 改进的查找逻辑"""
        # 方法1: 使用方法和路径
        key1 = f"{endpoint.method} {endpoint.path}"
        if key1 in self.existing_pages_cache:
            logger.debug(f"通过方法和路径找到页面: {key1}")
            return self.existing_pages_cache[key1]

        # 方法2: 使用接口名称
        if endpoint.summary:
            key2 = f"name:{endpoint.summary}"
            if key2 in self.existing_pages_cache:
                logger.debug(f"通过接口名称找到页面: {key2}")
                return self.existing_pages_cache[key2]

        # 方法3: 使用方法和路径的变体（处理可能的格式差异）
        # 例如：移除路径中的斜杠差异
        normalized_path = endpoint.path.rstrip('/')
        key3 = f"{endpoint.method} {normalized_path}"
        if key3 in self.existing_pages_cache:
            logger.debug(f"通过规范化路径找到页面: {key3}")
            return self.existing_pages_cache[key3]

        # 方法4: 如果没有摘要，尝试用方法和路径生成名称
        fallback_name = f"{endpoint.method} {endpoint.path}"
        key4 = f"name:{fallback_name}"
        if key4 in self.existing_pages_cache:
            logger.debug(f"通过回退名称找到页面: {key4}")
            return self.existing_pages_cache[key4]

        logger.debug(f"未找到已存在的页面: {endpoint.method} {endpoint.path}")
        return None

    def generate_markdown_content(self, endpoint: EndpointInfo) -> str:
        """为接口生成 Markdown 文档内容"""
        md_content = []

        md_content.append(f"# {endpoint.method} {endpoint.path}")
        md_content.append("")

        if endpoint.summary:
            md_content.append(f"**摘要**: {endpoint.summary}")
            md_content.append("")

        if endpoint.description:
            md_content.append(f"**描述**: {endpoint.description}")
            md_content.append("")

        if endpoint.tags:
            md_content.append(f"**标签**: {', '.join(endpoint.tags)}")
            md_content.append("")

        if endpoint.security:
            md_content.append("**安全要求**:")
            for sec in endpoint.security:
                for sec_name, sec_scopes in sec.items():
                    md_content.append(f"- {sec_name}: {', '.join(sec_scopes)}")
            md_content.append("")

        if endpoint.parameters:
            path_params = [p for p in endpoint.parameters if p.param_type == 'path']
            query_params = [p for p in endpoint.parameters if p.param_type == 'query']
            header_params = [p for p in endpoint.parameters if p.param_type == 'header']
            body_params = [p for p in endpoint.parameters if p.param_type == 'body']

            md_content.append("## 参数")
            md_content.append("")

            if path_params:
                md_content.append("### 路径参数")
                md_content.append("")
                self.add_parameter_table(md_content, path_params)

            if query_params:
                md_content.append("### 查询参数")
                md_content.append("")
                self.add_parameter_table(md_content, query_params)

            if header_params:
                md_content.append("### 请求头参数")
                md_content.append("")
                self.add_parameter_table(md_content, header_params)

            if body_params:
                md_content.append("### 请求体参数")
                md_content.append("")
                self.add_parameter_table(md_content, body_params)

                md_content.append("#### 请求体示例")
                md_content.append("")
                md_content.append("```json")
                request_body_example = self.generate_request_body_example(body_params)
                md_content.append(request_body_example)
                md_content.append("```")
                md_content.append("")

        md_content.append("## 请求示例")
        md_content.append("")
        md_content.append("```bash")
        md_content.append(self.generate_curl_example(endpoint))
        md_content.append("```")
        md_content.append("")

        md_content.append("## 响应")
        md_content.append("")

        success_response = next((r for r in endpoint.responses if r.status_code.startswith('2')), None)
        if success_response:
            md_content.append(f"### 成功响应 ({success_response.status_code})")
            md_content.append("")
            md_content.append(f"**描述**: {success_response.description}")
            md_content.append("")
            md_content.append("```json")
            md_content.append(self.generate_response_example(endpoint, success_response.status_code))
            md_content.append("```")
            md_content.append("")

        error_responses = [r for r in endpoint.responses if not r.status_code.startswith('2')]
        if error_responses:
            md_content.append("### 错误响应")
            md_content.append("")
            for error_response in error_responses:
                md_content.append(f"#### {error_response.status_code}")
                md_content.append("")
                md_content.append(f"**描述**: {error_response.description}")
                md_content.append("")
                md_content.append("```json")
                md_content.append(self.generate_error_response_example(endpoint, error_response.status_code))
                md_content.append("```")
                md_content.append("")

        return "\n".join(md_content)

    def add_parameter_table(self, md_content: List[str], parameters: List[ParameterInfo]):
        """添加参数表格到 Markdown 内容"""
        if not parameters:
            return

        md_content.append("| 参数名 | 类型 | 是否必需 | 描述 | 示例 | 默认值 |")
        md_content.append("|-------|------|---------|------|------|--------|")

        for param in parameters:
            required = "是" if param.required else "否"
            example = self.format_example_value(param.example)
            default = self.format_example_value(param.default)
            param_type = self.get_parameter_type(param)

            md_content.append(
                f"| {param.name} | {param_type} | {required} | {param.description} | {example} | {default} |")

        md_content.append("")

    def get_parameter_type(self, param: ParameterInfo) -> str:
        """获取参数的数据类型"""
        if param.enum:
            enum_values = ", ".join([str(v) for v in param.enum])
            return f"枚举: [{enum_values}]"

        param_name_lower = param.name.lower()
        param_desc_lower = param.description.lower() if param.description else ""

        type_mappings = {
            'id': 'string', 'uuid': 'string',
            'name': 'string', 'description': 'string', 'title': 'string',
            'count': 'integer', 'size': 'integer', 'page': 'integer', 'limit': 'integer',
            'amount': 'number', 'price': 'number', 'rate': 'number',
            'active': 'boolean', 'enabled': 'boolean', 'is_': 'boolean'
        }

        for key, param_type in type_mappings.items():
            if key in param_name_lower:
                return param_type

        if any(word in param_desc_lower for word in ['数组', 'array', '列表', 'list']):
            return "array"
        elif any(word in param_desc_lower for word in ['对象', 'object', 'json']):
            return "object"
        else:
            return "string"

    def format_example_value(self, value: Any) -> str:
        """格式化示例值"""
        if value is None:
            return ""

        if isinstance(value, list):
            try:
                return json.dumps(value, ensure_ascii=False)
            except:
                return str(value)

        try:
            return json.dumps(value, ensure_ascii=False)
        except:
            return str(value)

    def generate_request_body_example(self, body_params: List[ParameterInfo]) -> str:
        """生成请求体示例"""
        example = {}

        for param in body_params:
            if param.example is not None:
                example[param.name] = param.example
            elif param.default is not None:
                example[param.name] = param.default
            else:
                param_type = self.get_parameter_type(param)
                type_defaults = {
                    "integer": 0,
                    "number": 0.0,
                    "boolean": True,
                    "array": [],
                    "object": {}
                }
                example[param.name] = type_defaults.get(param_type, f"示例{param.name}")

        return json.dumps(example, ensure_ascii=False, indent=2)

    def generate_curl_example(self, endpoint: EndpointInfo) -> str:
        """生成 cURL 请求示例"""
        base_url = self.swagger_data.get('host', 'https://your-api-domain.com')
        if 'basePath' in self.swagger_data:
            base_url += self.swagger_data['basePath']

        url = base_url + endpoint.path

        for param in endpoint.parameters:
            if param.param_type == 'path':
                placeholder = f"[{param.name}]"
                if param.example is not None:
                    placeholder = str(param.example)
                url = url.replace(f"{{{param.name}}}", placeholder)

        query_params = []
        for param in endpoint.parameters:
            if param.param_type == 'query':
                value = param.example if param.example is not None else param.default
                if value is not None:
                    query_params.append(f"{param.name}={value}")

        if query_params:
            url += "?" + "&".join(query_params)

        curl_cmd = f"curl -X {endpoint.method}"

        content_type = endpoint.consumes[0] if endpoint.consumes else "application/json"
        curl_cmd += f" -H 'Content-Type: {content_type}'"

        if endpoint.security:
            for sec in endpoint.security:
                for sec_name in sec:
                    if sec_name in self.security_definitions:
                        auth_def = self.security_definitions[sec_name]
                        if auth_def['type'] == 'apiKey' and auth_def['in'] == 'header':
                            curl_cmd += f" -H '{auth_def['name']}: Bearer YOUR_TOKEN_HERE'"

        body_params = [p for p in endpoint.parameters if p.param_type == 'body']
        request_body = {}
        for param in body_params:
            if param.example is not None:
                request_body[param.name] = param.example
            elif param.default is not None:
                request_body[param.name] = param.default

        if endpoint.method not in ['GET', 'HEAD'] and request_body:
            body_json = json.dumps(request_body, ensure_ascii=False)
            body_json_escaped = body_json.replace("'", "'\\''")
            curl_cmd += f" -d '{body_json_escaped}'"

        curl_cmd += f" '{url}'"

        return curl_cmd

    def generate_response_example(self, endpoint: EndpointInfo, status_code: str = "200") -> str:
        """生成响应示例"""
        response = next((r for r in endpoint.responses if r.status_code == status_code), None)
        if not response:
            return "无示例数据"

        if response.example:
            return json.dumps(response.example, ensure_ascii=False, indent=2)

        if response.schema_ref and response.schema_ref in self.schemas:
            schema = self.schemas[response.schema_ref]
            example = {}

            for prop in schema.properties:
                if prop.example is not None:
                    example[prop.name] = prop.example
                elif prop.default is not None:
                    example[prop.name] = prop.default
                else:
                    type_defaults = {
                        'string': "示例字符串",
                        'integer': 0,
                        'number': 0.0,
                        'boolean': True,
                        'array': [],
                        'object': {}
                    }
                    example[prop.name] = type_defaults.get(prop.prop_type, {})

            return json.dumps(example, ensure_ascii=False, indent=2)

        return "无示例数据"

    def generate_error_response_example(self, endpoint: EndpointInfo, status_code: str) -> str:
        """生成错误响应示例"""
        error_response = next((r for r in endpoint.responses if r.status_code == status_code), None)
        if not error_response:
            return json.dumps({
                "code": 1030400,
                "errCode": 1030400,
                "message": "请求参数错误",
                "data": None
            }, ensure_ascii=False, indent=2)

        if error_response.example:
            return json.dumps(error_response.example, ensure_ascii=False, indent=2)

        if error_response.schema_ref and error_response.schema_ref in self.schemas:
            schema = self.schemas[error_response.schema_ref]
            example = {}

            for prop in schema.properties:
                if prop.example is not None:
                    example[prop.name] = prop.example
                elif prop.default is not None:
                    example[prop.name] = prop.default
                else:
                    type_defaults = {
                        'string': "错误信息",
                        'integer': int(status_code) if status_code.isdigit() else 1030400,
                        'number': 0.0,
                        'boolean': False,
                        'array': [],
                        'object': {}
                    }
                    example[prop.name] = type_defaults.get(prop.prop_type, {})

            return json.dumps(example, ensure_ascii=False, indent=2)

        error_code = int(status_code) if status_code.isdigit() else 1030400
        return json.dumps({
            "code": error_code,
            "errCode": error_code,
            "message": error_response.description or "操作失败",
            "data": None
        }, ensure_ascii=False, indent=2)

    def markdown_to_notion_blocks(self, markdown_text: str) -> List[Dict]:
        """将 Markdown 文本转换为 Notion 块结构 - 修复表格重复问题"""
        blocks = []
        lines = markdown_text.split('\n')

        # 调试：记录原始 Markdown 内容
        logger.debug("=== 原始 Markdown 内容 ===")
        for i, line in enumerate(lines):
            logger.debug(f"{i}: {line}")
        logger.debug("=========================")

        i = 0
        processed_lines = set()  # 跟踪已处理的行号

        while i < len(lines):
            if i in processed_lines:
                i += 1
                continue

            line = lines[i].strip()

            if not line:
                i += 1
                continue

            # 处理标题
            if line.startswith('#'):
                level = min(line.count('#'), 3)
                content = line.lstrip('#').strip()
                blocks.append({
                    "object": "block",
                    "type": f"heading_{level}",
                    f"heading_{level}": {
                        "rich_text": [{"type": "text", "text": {"content": content}}]
                    }
                })
                processed_lines.add(i)

            # 处理代码块
            elif line.startswith('```'):
                language = line[3:].strip() or 'plain text'
                code_lines = []
                i += 1
                processed_lines.add(i - 1)

                while i < len(lines) and not lines[i].strip().startswith('```'):
                    code_lines.append(lines[i])
                    processed_lines.add(i)
                    i += 1

                # 添加结束的 ```
                if i < len(lines) and lines[i].strip().startswith('```'):
                    processed_lines.add(i)
                    i += 1

                code_content = '\n'.join(code_lines).strip()

                if len(code_content) > 2000:
                    code_parts = self.split_long_text(code_content, max_length=1900)
                    for part in code_parts:
                        blocks.append({
                            "object": "block",
                            "type": "code",
                            "code": {
                                "rich_text": [{"type": "text", "text": {"content": part}}],
                                "language": self.get_supported_language(language)
                            }
                        })
                else:
                    blocks.append({
                        "object": "block",
                        "type": "code",
                        "code": {
                            "rich_text": [{"type": "text", "text": {"content": code_content}}],
                            "language": self.get_supported_language(language)
                        }
                    })
                continue  # 不要执行 i += 1，因为我们已经手动增加了 i

            # 处理表格 - 完全重写表格处理逻辑
            elif self.is_parameter_table_header(line):
                logger.debug(f"检测到表格表头，行 {i}: {line}")
                table_data, last_index = self.extract_complete_table_data(lines, i)
                logger.debug(f"提取的表格数据行数: {len(table_data)}")

                if table_data and len(table_data) >= 2:
                    # 标记所有表格行为已处理
                    for table_line_index in range(i, last_index + 1):
                        processed_lines.add(table_line_index)

                    table_block = self.create_notion_table_block(table_data)
                    if table_block:
                        blocks.append(table_block)
                        logger.debug(f"表格处理完成，跳转到行 {last_index + 1}")
                        i = last_index + 1  # 跳转到表格后的下一行
                        continue
                else:
                    logger.debug(f"表格数据无效，按普通文本处理")
                    text_parts = self.parse_inline_formatting(line)
                    if text_parts:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": text_parts
                            }
                        })
                    processed_lines.add(i)

            # 处理普通段落
            else:
                # 检查是否是孤立的表格行
                if '|' in line and self.is_isolated_table_line(line, i, lines, processed_lines):
                    logger.debug(f"检测到孤立表格行，行 {i}: {line}")
                    # 将孤立的表格行转换为普通段落
                    text_parts = self.parse_inline_formatting(line)
                    if text_parts:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": text_parts
                            }
                        })
                else:
                    text_parts = self.parse_inline_formatting(line)
                    if text_parts:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": text_parts
                            }
                        })
                processed_lines.add(i)

            i += 1

        # 调试：记录生成的块
        logger.debug("=== 生成的 Notion 块 ===")
        for j, block in enumerate(blocks):
            block_type = block.get('type', 'unknown')
            if block_type == 'table':
                logger.debug(f"块 {j}: {block_type} (行数: {len(block.get('table', {}).get('children', []))})")
            else:
                logger.debug(f"块 {j}: {block_type}")
        logger.debug("=======================")

        return blocks

    def is_isolated_table_line(self, line: str, current_index: int, all_lines: List[str], processed_lines: set) -> bool:
        """检查是否是孤立的表格行（不应该被当作表格处理）"""
        if '|' not in line:
            return False

        # 如果这一行已经被标记为表格的一部分，则不是孤立行
        if current_index in processed_lines:
            return False

        # 检查前后行是否是表格的一部分
        prev_index = current_index - 1
        next_index = current_index + 1

        prev_is_table = False
        next_is_table = False

        # 检查前一行
        if prev_index >= 0 and prev_index not in processed_lines:
            prev_line = all_lines[prev_index].strip()
            prev_is_table = (self.is_parameter_table_header(prev_line) or
                             self.is_table_separator(prev_line) or
                             ('|' in prev_line and self.looks_like_table_row(prev_line)))

        # 检查后一行
        if next_index < len(all_lines) and next_index not in processed_lines:
            next_line = all_lines[next_index].strip()
            next_is_table = (self.is_parameter_table_header(next_line) or
                             self.is_table_separator(next_line) or
                             ('|' in next_line and self.looks_like_table_row(next_line)))

        # 如果前后行都不是表格行，则当前行是孤立表格行
        return not (prev_is_table or next_is_table)

    def extract_complete_table_data(self, lines: List[str], start_index: int) -> tuple:
        """提取完整的表格数据，正确处理所有行"""
        table_data = []
        i = start_index

        # 首先收集表头
        header_line = lines[i].strip()
        if '|' in header_line:
            header_cells = self.parse_table_row(header_line)
            if header_cells:
                table_data.append(header_cells)
                logger.debug(f"表头: {header_cells}")

        # 检查下一行是否是分隔行
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if self.is_table_separator(next_line):
                logger.debug("跳过分隔行")
                i += 1  # 跳过分隔行

        # 收集数据行
        i += 1
        while i < len(lines):
            line = lines[i].strip()

            # 如果是空行，停止收集
            if not line:
                break

            # 检查是否是表格数据行
            if '|' in line and not self.is_table_separator(line):
                cells = self.parse_table_row(line)
                if cells and len(cells) == len(table_data[0]):  # 确保列数一致
                    table_data.append(cells)
                    logger.debug(f"数据行 {i}: {cells}")
                else:
                    logger.debug(f"列数不一致或不是表格行，停止收集: {cells}")
                    break  # 列数不一致，停止收集
            else:
                logger.debug(f"不是表格行，停止收集: {line}")
                break  # 不是表格行，停止收集

            i += 1

        # 返回表格数据和最后处理的行索引
        return table_data, i - 1

    def is_parameter_table_header(self, line: str) -> bool:
        """判断是否为参数表格表头 - 更严格的检测"""
        line = line.strip()
        if '|' not in line:
            return False

        # 检查是否包含参数表格的关键词
        table_keywords = ['参数名', '名称', '字段', '参数', '属性', '类型', '是否必需', '描述', '示例', '默认值']
        header_contains_keywords = any(keyword in line for keyword in table_keywords)

        # 检查是否有足够多的列分隔符（至少3列）
        has_enough_columns = line.count('|') >= 3

        return header_contains_keywords and has_enough_columns

    def is_table_separator(self, line: str) -> bool:
        """判断是否为表格分隔行"""
        line = line.strip()
        if '|' not in line:
            return False

        # 检查是否是分隔行（主要包含 - 和 |）
        cells = [cell.strip() for cell in line.split('|') if cell.strip()]
        return all(re.match(r'^[\s\-]*$', cell) for cell in cells)

    def looks_like_table_row(self, line: str) -> bool:
        """检查一行是否看起来像表格行"""
        if '|' not in line:
            return False

        # 检查是否包含表格关键词或者有多个列
        cells = [cell.strip() for cell in line.split('|') if cell.strip()]
        return len(cells) >= 3  # 至少有3列才认为是表格行

    def parse_table_row(self, line: str) -> List[str]:
        """解析表格行"""
        cells = [cell.strip() for cell in line.split('|')]
        # 移除首尾的空单元格
        if cells and not cells[0]:
            cells = cells[1:]
        if cells and not cells[-1]:
            cells = cells[:-1]
        return cells

    def create_notion_table_block(self, table_data: List[List[str]]) -> Dict:
        """创建 Notion 表格块"""
        if len(table_data) < 2:
            return None

        # 确定表格列数
        column_count = len(table_data[0])

        # 创建表格行
        table_rows = []
        for row_idx, row in enumerate(table_data):
            table_row = {
                "type": "table_row",
                "table_row": {
                    "cells": []
                }
            }

            # 填充单元格
            for cell_idx in range(column_count):
                if cell_idx < len(row):
                    cell_content = row[cell_idx]
                else:
                    cell_content = ""

                # 检查是否为必需字段（第三列，索引为2）且值为"是"
                is_required_field = (cell_idx == 2 and row_idx > 0 and cell_content == "是")

                if is_required_field:
                    cell_content_blocks = [{
                        "type": "text",
                        "text": {"content": cell_content},
                        "annotations": {
                            "bold": True,
                            "color": "red"
                        }
                    }]
                else:
                    cell_content_blocks = [{
                        "type": "text",
                        "text": {"content": cell_content}
                    }]

                table_row["table_row"]["cells"].append(cell_content_blocks)

            table_rows.append(table_row)

        # 创建表格块
        table_block = {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": column_count,
                "has_column_header": True,
                "has_row_header": False,
                "children": table_rows
            }
        }

        return table_block

    def split_long_text(self, text: str, max_length: int = 1900) -> List[str]:
        """将长文本分割成多个部分"""
        if len(text) <= max_length:
            return [text]

        parts = []
        while text:
            if len(text) <= max_length:
                parts.append(text)
                break

            split_index = text.rfind('\n', 0, max_length)
            if split_index == -1:
                split_index = max_length

            parts.append(text[:split_index])
            text = text[split_index:].lstrip('\n')

        return parts

    def get_supported_language(self, language: str) -> str:
        """获取支持的编程语言"""
        supported_languages = [
            'abap', 'arduino', 'bash', 'basic', 'c', 'clojure', 'coffeescript',
            'c++', 'c#', 'css', 'dart', 'diff', 'docker', 'elixir', 'elm',
            'erlang', 'flow', 'fortran', 'f#', 'gherkin', 'glsl', 'go',
            'graphql', 'groovy', 'haskell', 'html', 'java', 'javascript',
            'json', 'julia', 'kotlin', 'latex', 'less', 'lisp', 'livescript',
            'lua', 'makefile', 'markdown', 'markup', 'matlab', 'mermaid',
            'nix', 'objective-c', 'ocaml', 'pascal', 'perl', 'php',
            'plain text', 'powershell', 'prolog', 'protobuf', 'python',
            'r', 'reason', 'ruby', 'rust', 'sass', 'scala', 'scheme',
            'scss', 'shell', 'sql', 'swift', 'typescript', 'vb.net',
            'verilog', 'vhdl', 'visual basic', 'webassembly', 'xml', 'yaml'
        ]

        return language if language in supported_languages else 'plain text'

    def parse_inline_formatting(self, text: str) -> List[Dict]:
        """解析内联格式"""
        if not text.strip():
            return []

        parts = []
        remaining_text = text

        while '`' in remaining_text:
            start = remaining_text.find('`')
            end = remaining_text.find('`', start + 1)

            if end == -1:
                break

            if start > 0:
                parts.append({
                    "type": "text",
                    "text": {"content": remaining_text[:start]}
                })

            code_text = remaining_text[start + 1:end]
            parts.append({
                "type": "text",
                "text": {"content": code_text},
                "annotations": {"code": True}
            })

            remaining_text = remaining_text[end + 1:]

        if not parts:
            remaining_text = text
            while '**' in remaining_text:
                start = remaining_text.find('**')
                end = remaining_text.find('**', start + 2)

                if end == -1:
                    break

                if start > 0:
                    parts.append({
                        "type": "text",
                        "text": {"content": remaining_text[:start]}
                    })

                bold_text = remaining_text[start + 2:end]
                parts.append({
                    "type": "text",
                    "text": {"content": bold_text},
                    "annotations": {"bold": True}
                })

                remaining_text = remaining_text[end + 2:]

        if remaining_text:
            parts.append({
                "type": "text",
                "text": {"content": remaining_text}
            })

        if not parts:
            parts = [{"type": "text", "text": {"content": text}}]

        return parts

    def sync_endpoint_batch(self, endpoints_batch: List[EndpointInfo]) -> tuple:
        """批量同步接口到 Notion"""
        success_count = 0
        error_count = 0

        for endpoint in endpoints_batch:
            try:
                self.sync_endpoint(endpoint)
                success_count += 1
                logger.info(f"✓ 成功同步: {endpoint.method} {endpoint.path}")
                time.sleep(self.delay_between_requests)
            except Exception as e:
                error_count += 1
                logger.error(f"✗ 同步失败: {endpoint.method} {endpoint.path} - {str(e)}")

        return success_count, error_count

    def sync_to_notion(self):
        """同步所有接口到 Notion"""
        logger.info("开始同步到 Notion...")
        start_time = time.time()

        self.get_existing_pages()

        # 分批处理接口
        total_endpoints = len(self.endpoints)
        batches = [self.endpoints[i:i + self.batch_size] for i in range(0, total_endpoints, self.batch_size)]

        total_success = 0
        total_error = 0

        logger.info(f"📦 总共 {total_endpoints} 个接口，分成 {len(batches)} 批处理")

        for i, batch in enumerate(batches, 1):
            logger.info(f"🔄 正在处理第 {i}/{len(batches)} 批 ({len(batch)} 个接口)")
            batch_start = time.time()

            success, error = self.sync_endpoint_batch(batch)
            total_success += success
            total_error += error

            batch_time = time.time() - batch_start
            logger.info(f"✅ 第 {i} 批处理完成，耗时 {batch_time:.2f} 秒")

            if i < len(batches):
                time.sleep(1)

        total_time = time.time() - start_time
        logger.info(f"🎉 同步完成! 成功 {total_success} 个, 失败 {total_error} 个, 总耗时 {total_time:.2f} 秒")

    def sync_endpoint(self, endpoint: EndpointInfo):
        """同步单个接口到 Notion - 优化内容更新速度"""
        properties = self.build_safe_properties(endpoint)

        # 使用改进的查找逻辑
        page_id = self.find_existing_page_id(endpoint)

        if page_id:
            logger.info(f"更新已存在的页面: {endpoint.method} {endpoint.path}")
            try:
                self.notion.pages.update(
                    page_id=page_id,
                    properties=properties
                )
                self.current_page_id = page_id

                # 优化：快速清空页面内容
                self.fast_clear_page_content(page_id)

            except Exception as e:
                logger.warning(f"更新页面失败，尝试创建新页面: {e}")
                # 如果更新失败，创建新页面
                page_id = None

        if not page_id:
            logger.info(f"创建新页面: {endpoint.method} {endpoint.path}")
            try:
                new_page = self.notion.pages.create(
                    parent={"database_id": self.database_id},
                    properties=properties
                )
                page_id = new_page["id"]
                self.current_page_id = page_id
                # 添加到缓存
                key = f"{endpoint.method} {endpoint.path}"
                self.existing_pages_cache[key] = page_id
                if endpoint.summary:
                    self.existing_pages_cache[f"name:{endpoint.summary}"] = page_id
            except Exception as e:
                logger.error(f"创建页面失败: {e}")
                return

        # 生成内容并批量添加
        markdown_content = self.generate_markdown_content(endpoint)
        blocks = self.markdown_to_notion_blocks(markdown_content)

        # 分批添加块
        chunk_size = 50
        for i in range(0, len(blocks), chunk_size):
            chunk = blocks[i:i + chunk_size]
            try:
                self.notion.blocks.children.append(
                    block_id=page_id,
                    children=chunk
                )
            except Exception as e:
                logger.warning(f"添加块失败: {e}")

    def fast_clear_page_content(self, page_id: str):
        """快速清空页面内容 - 优化版本"""
        logger.debug(f"快速清空页面内容: {page_id}")
        try:
            # 获取所有块
            existing_blocks = self.notion.blocks.children.list(block_id=page_id)

            if not existing_blocks['results']:
                return

            block_ids = [block['id'] for block in existing_blocks['results']]

            # 使用批量删除（虽然Notion API没有官方批量删除，但我们可以使用并发）
            # 由于Notion API限制，我们仍然需要逐个删除，但可以优化删除顺序
            # 先删除子块，再删除父块，避免可能的依赖问题

            # 按块类型排序，先删除简单块
            sorted_blocks = sorted(existing_blocks['results'],
                                   key=lambda x: self.get_block_priority(x.get('type', '')))

            # 批量删除（使用小批次避免速率限制）
            batch_size = 10
            for i in range(0, len(sorted_blocks), batch_size):
                batch = sorted_blocks[i:i + batch_size]

                # 使用线程池并行删除（可选，但要注意API限制）
                for block in batch:
                    try:
                        self.notion.blocks.delete(block_id=block['id'])
                    except Exception as e:
                        logger.debug(f"删除块失败 {block['id']}: {e}")
                        # 继续删除其他块，不因单个失败而停止
                        continue

                # 小延迟避免速率限制
                if i + batch_size < len(sorted_blocks):
                    time.sleep(0.1)

        except Exception as e:
            logger.warning(f"快速清空页面内容失败: {e}")
            # 回退到原来的逐个删除方法
            self.slow_clear_page_content(page_id)

    def get_block_priority(self, block_type: str) -> int:
        """获取块删除优先级，数值越小优先级越高"""
        priority_map = {
            'paragraph': 1,
            'heading_1': 2,
            'heading_2': 3,
            'heading_3': 4,
            'bulleted_list_item': 5,
            'numbered_list_item': 6,
            'to_do': 7,
            'toggle': 8,
            'code': 9,
            'quote': 10,
            'callout': 11,
            'table': 12,  # 表格最后删除
            'table_row': 13,
            'column_list': 14,
            'column': 15
        }
        return priority_map.get(block_type, 20)

    def slow_clear_page_content(self, page_id: str):
        """慢速但可靠的清空页面内容方法（备用）"""
        logger.debug(f"使用慢速方法清空页面内容: {page_id}")
        try:
            existing_blocks = self.notion.blocks.children.list(block_id=page_id)
            if existing_blocks['results']:
                for block in existing_blocks['results']:
                    try:
                        self.notion.blocks.delete(block_id=block['id'])
                        time.sleep(0.05)  # 小延迟避免速率限制
                    except Exception as e:
                        logger.debug(f"删除块失败 {block['id']}: {e}")
        except Exception as e:
            logger.warning(f"清空页面内容失败: {e}")

    def build_safe_properties(self, endpoint: EndpointInfo) -> Dict:
        """构建安全的属性字典"""
        properties = {}

        basic_properties = {
            "接口名称": {
                "title": [{"text": {"content": endpoint.summary or f"{endpoint.method} {endpoint.path}"}}]
            },
            "接口路径": {
                "rich_text": [{"text": {"content": endpoint.path}}]
            },
            "HTTP方法": {
                "select": {"name": endpoint.method}
            },
            "标签": {
                "multi_select": [{"name": tag} for tag in endpoint.tags]
            }
        }

        for prop_name, prop_value in basic_properties.items():
            if prop_name in self.database_properties:
                properties[prop_name] = prop_value

        optional_properties = {}

        if "状态" in self.database_properties:
            optional_properties["状态"] = {"status": {"name": "✅ 活跃"}}

        if "最后同步时间" in self.database_properties:
            optional_properties["最后同步时间"] = {"date": {"start": datetime.now().isoformat()}}

        if "参数数量" in self.database_properties:
            optional_properties["参数数量"] = {"number": len(endpoint.parameters)}

        if "是否需要认证" in self.database_properties:
            optional_properties["是否需要认证"] = {"checkbox": bool(endpoint.security)}

        if endpoint.description and "描述" in self.database_properties:
            optional_properties["描述"] = {"rich_text": [{"text": {"content": endpoint.description}}]}

        if endpoint.tags and "分类" in self.database_properties:
            optional_properties["分类"] = {"select": {"name": endpoint.tags[0]}}

        if endpoint.security and "安全性" in self.database_properties:
            security_types = []
            for sec in endpoint.security:
                security_types.extend(list(sec.keys()))
            if security_types:
                optional_properties["安全性"] = {"select": {"name": security_types[0]}}

        if endpoint.consumes and "请求格式" in self.database_properties:
            optional_properties["请求格式"] = {
                "multi_select": [{"name": content_type} for content_type in endpoint.consumes]
            }

        if endpoint.produces and "响应格式" in self.database_properties:
            optional_properties["响应格式"] = {
                "multi_select": [{"name": content_type} for content_type in endpoint.produces]
            }

        properties.update(optional_properties)

        return properties


def main():
    # 配置信息 - 使用固定数据库名称
    NOTION_TOKEN = ""
    PARENT_PAGE_ID = "298a4873d0648180a1bdc662570f5518"  # 使用不带连字符的ID格式
    # DATABASE_NAME = "WebPortal API 接口文档"  # 固定名称，确保重复使用
    DATABASE_NAME = "WebPortal 1.0 API Documentation"  # 固定名称，确保重复使用
    SWAGGER_FILE = "swagger.yaml"

    logger.info("🚀 Swagger to Notion 同步工具")
    logger.info("=" * 50)

    sync = SwaggerToNotionSync(
        notion_token=NOTION_TOKEN,
        parent_page_id=PARENT_PAGE_ID,
        database_name=DATABASE_NAME
    )

    try:
        sync.load_swagger_file(SWAGGER_FILE)
        sync.sync_to_notion()

        logger.info(f"\n📊 数据库 ID: {sync.database_id}")
        logger.info("💡 你可以在 Notion 中查看自动创建的 API 接口文档")

    except Exception as e:
        logger.error(f"❌ 同步失败: {str(e)}")


if __name__ == "__main__":
    main()
import json
import requests
from notion_client import Client
from datetime import datetime
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth


class SwaggerToNotionSync:
    def __init__(self, notion_token, api_db_id, param_db_id, response_db_id):
        self.notion = Client(auth=notion_token)
        self.api_db_id = api_db_id
        self.param_db_id = param_db_id
        self.response_db_id = response_db_id
        self.existing_apis = self.get_existing_apis()
        self.swagger_data = None
        self.processed_definitions = set()  # 跟踪已处理的定义，避免循环引用

    def get_existing_apis(self):
        """获取Notion中已存在的API列表"""
        result = self.notion.databases.query(database_id=self.api_db_id)
        return {page['properties']['接口路径']['rich_text'][0]['text']['content']: page['id']
                for page in result['results']}

    def fetch_swagger_json(self, url, auth_token=None):
        """获取Swagger JSON文档"""
        headers = {'Accept': 'application/json'}
        # if auth_token:
        #     headers['Authorization'] = f'Bearer {auth_token}'


        if  auth_token.username:
            response = requests.get(url, auth=auth_token)
        else:
            headers['Authorization'] = f'Bearer {auth_token}'
            response = requests.get(url, headers=headers)

        response.raise_for_status()
        return response.json()

    def resolve_ref(self, ref_path):
        """解析 $ref 引用路径"""
        if not ref_path or not ref_path.startswith('#/'):
            return None

        parts = ref_path.split('/')[1:]  # 去掉开头的 '#/'
        current = self.swagger_data

        for part in parts:
            if part in current:
                current = current[part]
            else:
                return None
        return current

    def extract_properties_from_schema(self, schema, parent_name="", required_fields=None):
        """从schema中提取所有属性，包括嵌套属性"""
        properties = []
        required_fields = required_fields or []

        if not schema:
            return properties

        # 处理 $ref 引用
        if '$ref' in schema:
            ref_schema = self.resolve_ref(schema['$ref'])
            if ref_schema:
                ref_id = schema['$ref']
                if ref_id not in self.processed_definitions:
                    self.processed_definitions.add(ref_id)
                    return self.extract_properties_from_schema(
                        ref_schema,
                        parent_name,
                        ref_schema.get('required', [])
                    )
            return properties

        # 处理对象的properties
        if 'properties' in schema:
            for prop_name, prop_info in schema['properties'].items():
                full_name = f"{parent_name}.{prop_name}" if parent_name else prop_name
                is_required = prop_name in required_fields

                prop_data = {
                    "name": full_name,
                    "type": prop_info.get('type', 'object'),
                    "required": is_required,
                    "description": prop_info.get('description', ''),
                    "example": prop_info.get('example', ''),
                    "enum": prop_info.get('enum', [])
                }

                properties.append(prop_data)

                # 递归处理嵌套对象
                if 'properties' in prop_info or '$ref' in prop_info:
                    nested_props = self.extract_properties_from_schema(
                        prop_info,
                        full_name,
                        prop_info.get('required', [])
                    )
                    properties.extend(nested_props)

        return properties

    def sync_to_notion(self, swagger_url, auth_token=None):
        """主同步方法"""
        print("开始获取Swagger文档...")
        self.swagger_data = self.fetch_swagger_json(swagger_url, auth_token)

        print("开始解析定义...")
        # 首先解析所有定义，便于后续引用
        self.definitions = self.swagger_data.get('definitions', {})

        print("开始同步到Notion...")
        for path, path_info in self.swagger_data['paths'].items():
            for method, method_info in path_info.items():
                if method not in ['get', 'post', 'put', 'delete', 'patch']:
                    continue

                # 重置已处理定义集合
                self.processed_definitions.clear()

                # 创建或更新API接口
                api_page_id = self.create_or_update_api(path, method, method_info, swagger_url)

                # 同步参数
                self.sync_parameters(api_page_id, method_info)

                # 同步响应
                self.sync_responses(api_page_id, method_info)

        print("同步完成！")

    def create_or_update_api(self, path, method, method_info, swagger_url):
        """创建或更新API接口记录"""
        api_identifier = f"{method.upper()} {path}"

        properties = {
            "API 名称": {
                "title": [{"text": {"content": method_info.get('summary', api_identifier)}}]
            },
            "HTTP 方法": {
                "select": {"name": method.upper()}
            },
            "接口路径": {
                "rich_text": [{"text": {"content": path}}]
            },
            "标签": {
                "multi_select": [{"name": tag} for tag in method_info.get('tags', [])]
            },
            "概要": {
                "rich_text": [{"text": {"content": method_info.get('summary', '')}}]
            },
            "描述": {
                "rich_text": [{"text": {"content": method_info.get('description', '')}}]
            },
            "需要认证": {
                "checkbox": bool(method_info.get('security'))
            },
            "最后同步": {
                "date": {"start": datetime.now().isoformat()}
            },
            "Swagger 源": {
                "url": swagger_url
            },
            "状态": {
                "status": {"name": "✅ 活跃"}
            }
        }

        if path in self.existing_apis:
            page_id = self.existing_apis[path]
            self.notion.pages.update(page_id=page_id, properties=properties)
            print(f"更新接口: {api_identifier}")
            return page_id
        else:
            new_page = self.notion.pages.create(
                parent={"database_id": self.api_db_id},
                properties=properties
            )
            print(f"创建接口: {api_identifier}")
            self.existing_apis[path] = new_page['id']
            return new_page['id']

    def sync_parameters(self, api_page_id, method_info):
        """同步参数信息，包括解析$ref"""
        parameters = method_info.get('parameters', [])
        request_body = method_info.get('requestBody')

        # 处理普通参数
        for param in parameters:
            param_schema = param.get('schema', {})
            param_properties = []

            # 如果参数有schema，解析其中的属性
            if param_schema:
                param_properties = self.extract_properties_from_schema(
                    param_schema,
                    param['name'],
                    param_schema.get('required', [])
                )

            if param_properties:
                # 如果有嵌套属性，创建多个参数记录
                for prop in param_properties:
                    self.create_parameter(api_page_id, {
                        "name": prop['name'],
                        "in": param['in'],
                        "required": prop['required'],
                        "type": prop['type'],
                        "description": prop['description'],
                        "example": prop.get('example', ''),
                        "enum": prop.get('enum', [])
                    })
            else:
                # 普通参数
                self.create_parameter(api_page_id, {
                    "name": param['name'],
                    "in": param['in'],
                    "required": param.get('required', False),
                    "type": param_schema.get('type', 'string') if param_schema else 'string',
                    "description": param.get('description', ''),
                    "default": param.get('default', '')
                })

        # 处理请求体参数
        if request_body:
            content = request_body.get('content', {})
            json_schema = content.get('application/json', {}).get('schema', {})

            if json_schema:
                body_properties = self.extract_properties_from_schema(
                    json_schema,
                    "body",
                    json_schema.get('required', [])
                )

                for prop in body_properties:
                    self.create_parameter(api_page_id, {
                        "name": prop['name'],
                        "in": "body",
                        "required": prop['required'],
                        "type": prop['type'],
                        "description": prop['description'],
                        "example": prop.get('example', ''),
                        "enum": prop.get('enum', [])
                    })

    def create_parameter(self, api_page_id, param_data):
        """创建参数记录"""
        # 构建枚举值的显示文本
        enum_text = ""
        if param_data.get('enum'):
            enum_text = f"枚举值: {', '.join(map(str, param_data['enum']))}"

        description = param_data.get('description', '')
        if enum_text:
            description = f"{description}\n\n{enum_text}" if description else enum_text

        properties = {
            "参数名": {
                "title": [{"text": {"content": param_data['name']}}]
            },
            "所属接口": {
                "relation": [{"id": api_page_id}]
            },
            "参数位置": {
                "select": {"name": param_data['in']}
            },
            "是否必填": {
                "checkbox": param_data['required']
            },
            "数据类型": {
                "rich_text": [{"text": {"content": param_data['type']}}]
            },
            "描述": {
                "rich_text": [{"text": {"content": description}}]
            }
        }

        # 添加示例值（如果有）
        if param_data.get('example'):
            properties["示例值"] = {
                "rich_text": [{"text": {"content": str(param_data['example'])}}]
            }

        # 添加默认值（如果有）
        if param_data.get('default') is not None:
            properties["默认值"] = {
                "rich_text": [{"text": {"content": str(param_data['default'])}}]
            }

        self.notion.pages.create(
            parent={"database_id": self.param_db_id},
            properties=properties
        )

    def sync_responses(self, api_page_id, method_info):
        """同步响应信息，包括解析$ref"""
        responses = method_info.get('responses', {})

        for status_code, response_info in responses.items():
            schema = response_info.get('schema', {})
            response_properties = []

            # 解析响应schema中的属性
            if schema:
                response_properties = self.extract_properties_from_schema(schema)

            properties = {
                "响应状态": {
                    "title": [{"text": {"content": status_code}}]
                },
                "所属接口": {
                    "relation": [{"id": api_page_id}]
                },
                "描述": {
                    "rich_text": [{"text": {"content": response_info.get('description', '')}}]
                }
            }

            # 如果有详细的响应属性，添加到描述中
            if response_properties:
                props_description = "\n\n响应字段:\n" + "\n".join(
                    [f"- {prop['name']}: {prop['type']} {'(必填)' if prop['required'] else ''}"
                     for prop in response_properties]
                )
                properties["描述"]["rich_text"][0]["text"]["content"] += props_description

            self.notion.pages.create(
                parent={"database_id": self.response_db_id},
                properties=properties
            )

    def generate_interface_documentation(self):
        """生成完整的接口文档报告"""
        print("生成接口文档报告...")

        for path, path_info in self.swagger_data['paths'].items():
            for method, method_info in path_info.items():
                if method not in ['get', 'post', 'put', 'delete', 'patch']:
                    continue

                print(f"\n=== {method.upper()} {path} ===")
                print(f"摘要: {method_info.get('summary', '')}")
                print(f"描述: {method_info.get('description', '')}")
                print(f"标签: {', '.join(method_info.get('tags', []))}")

                # 解析请求参数
                print("\n请求参数:")
                self.processed_definitions.clear()
                parameters = method_info.get('parameters', [])
                request_body = method_info.get('requestBody')

                # 这里可以添加详细的参数解析输出...

                # 解析响应
                print("\n响应:")
                responses = method_info.get('responses', {})
                for status_code, response_info in responses.items():
                    print(f"  {status_code}: {response_info.get('description', '')}")




def swagger_url_test(NOTION_TOKEN,API_DB_ID):
    notion = Client(auth=NOTION_TOKEN)  # 不要使用OAuth2 token

    # 确保使用正确的数据库ID
    database_id = API_DB_ID  # 替换为正确的ID

    try:
        response = notion.databases.query(database_id=database_id)
        print("查询成功")
    except Exception as e:
        print(f"错误: {e}")


def test_notion(NOTION_TOKEN,API_DB_ID):
    print(111,NOTION_TOKEN)
    notion = Client(auth=NOTION_TOKEN)
    print(notion.databases)

    user_info = notion.users.me()
    print("认证成功！", user_info)

    dbs=notion.search()

    print("DB列表",dbs)

    # database_info = notion.databases.query(database_id=user_info['database_id'])
    database_info=notion.databases.retrieve(database_id=API_DB_ID)
    print(database_info)

    # # 测试1：列出所有有权限的数据库
    # try:
    #     response = notion.search(filter={"property": "object", "value": "database"})
    #     print("有权限的数据库列表:")
    #     for db in response.get("results", []):
    #         print(f"- {db['title'][0]['plain_text'] if db['title'] else '无标题'}: {db['id']}")
    # except Exception as e:
    #     print(f"搜索数据库失败: {e}")
    #
    # # 测试2：尝试直接获取该数据库信息
    # try:
    #     db_info = notion.databases.retrieve("259f1267-22e1-80e4-b871-000caf6d7157")
    #     print(f"数据库获取成功: {db_info['title'][0]['plain_text']}")
    # except Exception as e:
    #     print(f"获取数据库信息失败: {e}")



def test2(NOTION_TOKEN):
    # 配置信息
    token = NOTION_TOKEN
    database_id = "259f126722e180e4b871000caf6d7157"

    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }

    # 发起POST请求（查询数据库）
    response = requests.post(url, headers=headers)

    # 检查响应
    if response.status_code == 200:
        print("成功！")
        data = response.json()
        # 美观地打印返回的JSON数据
        print(json.dumps(data, indent=4))
    else:
        print(f"请求失败，状态码: {response.status_code}")
        print(f"错误信息: {response.text}")  # 这个信息对于调试非常重要！


def get_AUTH_TOKEN(AUTH_TOKEN):
    # url = "http://your-domain.com/v3/api-docs"
    url = "https://webportal-dev.vivalink.com/api/backend/doc/doc.json"

    resp = requests.get(url, auth=AUTH_TOKEN)
    print(AUTH_TOKEN)
    print(resp.status_code)
    print(resp.json())


# 使用示例
if __name__ == "__main__":
    # 配置信息
    NOTION_TOKEN = ""
    # 正确地将连字符添加到ID中
    # API_DB_ID = "259f1267-22e1-80e4-b871-000caf6d7157"

    # API_DB_ID = "259f126722e180e4b871000caf6d7157"
    API_DB_ID = "259f1267-22e1-80c1-9104-e0486aab53b1"
    PARAM_DB_ID = "259f126722e1805d860ce2f8ea9e06d7"
    RESPONSE_DB_ID = "259f126722e180f48d96c2bb0903ee56"
    SWAGGER_URL = "https://webportal-dev.vivalink.com/api/backend/doc/doc.json"
    AUTH_TOKEN = HTTPBasicAuth("root", "root")  # 如果需要登录认证

    # swagger_url_test(NOTION_TOKEN, API_DB_ID)
    # test_notion(NOTION_TOKEN, API_DB_ID)
    # test2(NOTION_TOKEN)
    # get_AUTH_TOKEN(AUTH_TOKEN)

    # 创建同步器并执行同步
    sync = SwaggerToNotionSync(NOTION_TOKEN, API_DB_ID, PARAM_DB_ID, RESPONSE_DB_ID)
    sync.sync_to_notion(SWAGGER_URL, AUTH_TOKEN)

    # 可选：生成控制台文档报告
    # sync.generate_interface_documentation()

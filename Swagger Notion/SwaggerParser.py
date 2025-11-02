import yaml
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import urllib.parse


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


class SwaggerParser:
    def __init__(self, yaml_file_path: str):
        self.yaml_file_path = yaml_file_path
        self.swagger_data = None
        self.endpoints = []
        self.schemas = {}
        self.security_definitions = {}

    def load_swagger_file(self):
        """加载并解析 Swagger YAML 文件"""
        with open(self.yaml_file_path, 'r', encoding='utf-8') as file:
            self.swagger_data = yaml.safe_load(file)

        # 解析安全定义
        if 'securityDefinitions' in self.swagger_data:
            self.security_definitions = self.swagger_data['securityDefinitions']

        # 解析数据模型
        if 'definitions' in self.swagger_data:
            self.parse_definitions()

        # 解析接口端点
        self.parse_paths()

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

                # 解析参数
                parameters = []
                if 'parameters' in method_info:
                    parameters = self.parse_parameters(method_info['parameters'])

                # 解析请求体
                if 'requestBody' in method_info or any(
                        p.get('in') == 'body' for p in method_info.get('parameters', [])):
                    body_params = self.parse_request_body(method_info)
                    parameters.extend(body_params)

                # 解析响应
                responses = self.parse_responses(method_info.get('responses', {}))

                # 解析安全要求
                security = method_info.get('security', [])

                # 解析consumes和produces
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
            # 获取参数类型
            param_type = param.get('in', 'query')

            # 获取参数示例和默认值
            example = param.get('example')
            default = param.get('default')

            # 如果参数有schema，从中获取更多信息
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

        # 处理Swagger 2.0的body参数
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

        # 处理OpenAPI 3.0的requestBody
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

            # 解析响应schema
            if 'schema' in response_info:
                schema_ref = response_info['schema'].get('$ref', '')
                if schema_ref:
                    response.schema_ref = schema_ref.split('/')[-1]

            # 解析响应示例
            if 'examples' in response_info:
                # 取第一个示例
                for example_name, example_value in response_info['examples'].items():
                    if 'value' in example_value:
                        response.example = example_value['value']
                        break
            elif 'example' in response_info:
                response.example = response_info['example']

            result.append(response)

        return result

    def generate_request_example(self, endpoint: EndpointInfo) -> str:
        """生成cURL请求示例"""
        lines = []

        # 确定内容类型
        content_type = "application/json"
        if endpoint.consumes:
            content_type = endpoint.consumes[0]

        # 构建基础URL
        base_url = "https://your-api-domain.com"  # 这里应该替换为实际的API域名
        url = base_url + endpoint.path

        # 处理路径参数和查询参数
        query_params = []
        path_params = []
        body_params = []
        header_params = []

        for param in endpoint.parameters:
            if param.param_type == 'query':
                if param.example is not None:
                    query_params.append(f"{param.name}={param.example}")
                elif param.default is not None:
                    query_params.append(f"{param.name}={param.default}")
            elif param.param_type == 'path':
                path_params.append(param.name)
            elif param.param_type == 'body':
                body_params.append(param)
            elif param.param_type == 'header':
                header_params.append(param)

        # 处理路径参数
        for param_name in path_params:
            param = next((p for p in endpoint.parameters if p.name == param_name and p.param_type == 'path'), None)
            if param and param.example is not None:
                url = url.replace(f"{{{param_name}}}", str(param.example))
            else:
                url = url.replace(f"{{{param_name}}}", f"[{param_name}]")

        # 处理查询参数
        if query_params:
            url += "?" + "&".join(query_params)

        # 构建cURL命令
        curl_cmd = f"curl -X {endpoint.method}"

        # 添加Content-Type头
        curl_cmd += f" -H 'Content-Type: {content_type}'"

        # 添加认证头
        if endpoint.security:
            for sec in endpoint.security:
                for sec_name in sec:
                    if sec_name in self.security_definitions:
                        auth_def = self.security_definitions[sec_name]
                        if auth_def['type'] == 'apiKey' and auth_def['in'] == 'header':
                            curl_cmd += f" -H '{auth_def['name']}: Bearer YOUR_TOKEN_HERE'"

        # 添加其他header参数
        for param in header_params:
            if param.example is not None:
                curl_cmd += f" -H '{param.name}: {param.example}'"
            elif param.default is not None:
                curl_cmd += f" -H '{param.name}: {param.default}'"

        # 构建请求体
        request_body = {}
        for param in body_params:
            if param.example is not None:
                request_body[param.name] = param.example
            elif param.default is not None:
                request_body[param.name] = param.default

        # 添加请求体（如果不是GET或HEAD）
        if endpoint.method not in ['GET', 'HEAD'] and request_body:
            # 将请求体转换为JSON字符串并转义
            body_json = json.dumps(request_body, ensure_ascii=False)
            # 转义单引号以便在shell中使用
            body_json_escaped = body_json.replace("'", "'\\''")
            curl_cmd += f" -d '{body_json_escaped}'"

        # 添加URL
        curl_cmd += f" '{url}'"

        lines.append(curl_cmd)
        lines.append("")

        # 添加说明
        lines.append("# 说明:")
        lines.append(f"# -X {endpoint.method}: 指定HTTP方法")
        lines.append(f"# -H 'Content-Type: {content_type}': 指定内容类型")

        if endpoint.security:
            for sec in endpoint.security:
                for sec_name in sec:
                    if sec_name in self.security_definitions:
                        auth_def = self.security_definitions[sec_name]
                        if auth_def['type'] == 'apiKey' and auth_def['in'] == 'header':
                            lines.append(
                                f"# -H '{auth_def['name']}: Bearer YOUR_TOKEN_HERE': 认证令牌，请替换为实际令牌")

        if endpoint.method not in ['GET', 'HEAD'] and request_body:
            lines.append("# -d '...': 请求体数据")

        lines.append(f"# '{url}': 请求URL")

        return "\n".join(lines)

    def generate_response_example(self, endpoint: EndpointInfo, status_code: str = "200") -> str:
        """生成响应示例"""
        response = next((r for r in endpoint.responses if r.status_code == status_code), None)
        if not response:
            return "无示例数据"

        # 如果有直接示例，使用它
        if response.example:
            return json.dumps(response.example, ensure_ascii=False, indent=2)

        # 如果有schema引用，尝试从schema生成示例
        if response.schema_ref and response.schema_ref in self.schemas:
            schema = self.schemas[response.schema_ref]
            example = {}

            for prop in schema.properties:
                if prop.example is not None:
                    example[prop.name] = prop.example
                elif prop.default is not None:
                    example[prop.name] = prop.default
                else:
                    # 根据类型生成默认值
                    if prop.prop_type == 'string':
                        example[prop.name] = "示例字符串"
                    elif prop.prop_type == 'integer':
                        example[prop.name] = 0
                    elif prop.prop_type == 'number':
                        example[prop.name] = 0.0
                    elif prop.prop_type == 'boolean':
                        example[prop.name] = True
                    elif prop.prop_type == 'array':
                        example[prop.name] = []
                    else:
                        example[prop.name] = {}

            return json.dumps(example, ensure_ascii=False, indent=2)

        return "无示例数据"

    def generate_error_response_example(self, endpoint: EndpointInfo) -> str:
        """生成错误响应示例"""
        error_responses = [r for r in endpoint.responses if r.status_code not in ['200', '201', '202']]

        if not error_responses:
            # 生成通用错误响应
            return json.dumps({
                "code": 1030400,
                "errCode": 1030400,
                "message": "请求参数错误",
                "data": None
            }, ensure_ascii=False, indent=2)

        # 使用第一个错误响应
        error_response = error_responses[0]

        if error_response.example:
            return json.dumps(error_response.example, ensure_ascii=False, indent=2)

        # 如果有schema引用，尝试从schema生成示例
        if error_response.schema_ref and error_response.schema_ref in self.schemas:
            schema = self.schemas[error_response.schema_ref]
            example = {}

            for prop in schema.properties:
                if prop.example is not None:
                    example[prop.name] = prop.example
                elif prop.default is not None:
                    example[prop.name] = prop.default
                else:
                    # 根据类型生成默认值
                    if prop.prop_type == 'string':
                        example[prop.name] = "错误信息"
                    elif prop.prop_type == 'integer':
                        example[prop.name] = 1030400  # 通用错误码
                    elif prop.prop_type == 'number':
                        example[prop.name] = 0.0
                    elif prop.prop_type == 'boolean':
                        example[prop.name] = False
                    elif prop.prop_type == 'array':
                        example[prop.name] = []
                    else:
                        example[prop.name] = {}

            return json.dumps(example, ensure_ascii=False, indent=2)

        # 生成通用错误响应
        error_code = int(error_response.status_code) if error_response.status_code.isdigit() else 1030400
        return json.dumps({
            "code": error_code,
            "errCode": error_code,
            "message": error_response.description or "操作失败",
            "data": None
        }, ensure_ascii=False, indent=2)

    def generate_markdown_documentation(self, output_file: str = None) -> str:
        """生成Markdown格式的文档"""
        md_content = []

        # 文档头部信息
        info = self.swagger_data.get('info', {})
        md_content.append(f"# {info.get('title', 'API Documentation')}")
        md_content.append("")
        md_content.append(f"**版本**: {info.get('version', '1.0')}")
        md_content.append("")
        md_content.append(f"**描述**: {info.get('description', '')}")
        md_content.append("")
        md_content.append(f"**服务条款**: {info.get('termsOfService', '')}")
        md_content.append("")

        # 认证与安全
        md_content.append("## 认证与安全")
        md_content.append("")
        md_content.append("### 认证方式")
        for auth_name, auth_info in self.security_definitions.items():
            md_content.append(
                f"- **{auth_name}**: 在请求头中使用 `{auth_info.get('name')}` 字段传递 {auth_info.get('type')}")
        md_content.append("")

        # 按标签分组接口
        tags = set()
        for endpoint in self.endpoints:
            tags.update(endpoint.tags)

        md_content.append("## 主要功能模块")
        md_content.append("")
        for i, tag in enumerate(sorted(tags), 1):
            md_content.append(f"{i}. **{tag}**")
        md_content.append("")

        # 详细接口说明
        for tag in sorted(tags):
            md_content.append(f"## {tag}")
            md_content.append("")

            # 获取该标签下的所有接口
            tag_endpoints = [e for e in self.endpoints if tag in e.tags]

            for endpoint in tag_endpoints:
                md_content.append(f"### {endpoint.method} {endpoint.path}")
                md_content.append("")

                if endpoint.summary:
                    md_content.append(f"**摘要**: {endpoint.summary}")
                    md_content.append("")

                if endpoint.description:
                    md_content.append(f"**描述**: {endpoint.description}")
                    md_content.append("")

                if endpoint.parameters:
                    md_content.append("**参数**:")
                    md_content.append("")
                    md_content.append("| 参数名 | 类型 | 是否必需 | 描述 | 示例 | 默认值 |")
                    md_content.append("|-------|------|---------|------|------|--------|")

                    for param in endpoint.parameters:
                        required = "是" if param.required else "否"
                        example = json.dumps(param.example) if param.example else ""
                        default = json.dumps(param.default) if param.default else ""
                        md_content.append(
                            f"| {param.name} | {param.param_type} | {required} | {param.description} | {example} | {default} |")

                    md_content.append("")

                # 请求示例
                md_content.append("**请求示例**:")
                md_content.append("")
                md_content.append("```bash")
                md_content.append(self.generate_request_example(endpoint))
                md_content.append("```")
                md_content.append("")

                # 成功响应示例
                success_response = next((r for r in endpoint.responses if r.status_code.startswith('2')), None)
                if success_response:
                    md_content.append("**成功响应示例**:")
                    md_content.append("")
                    md_content.append("```json")
                    md_content.append(self.generate_response_example(endpoint, success_response.status_code))
                    md_content.append("```")
                    md_content.append("")

                # 错误响应示例
                error_responses = [r for r in endpoint.responses if not r.status_code.startswith('2')]
                if error_responses:
                    md_content.append("**错误响应示例**:")
                    md_content.append("")
                    md_content.append("```json")
                    md_content.append(self.generate_error_response_example(endpoint))
                    md_content.append("```")
                    md_content.append("")

                if endpoint.security:
                    md_content.append("**安全要求**:")
                    for sec in endpoint.security:
                        for sec_name, sec_scopes in sec.items():
                            md_content.append(f"- {sec_name}: {', '.join(sec_scopes)}")
                    md_content.append("")

                md_content.append("---")
                md_content.append("")

        # 数据模型说明
        md_content.append("## 数据模型说明")
        md_content.append("")

        for schema_name, schema in self.schemas.items():
            md_content.append(f"### {schema_name}")
            md_content.append("")

            if schema.description:
                md_content.append(f"**描述**: {schema.description}")
                md_content.append("")

            if schema.properties:
                md_content.append("**属性**:")
                md_content.append("")
                md_content.append("| 属性名 | 类型 | 是否必需 | 描述 | 示例 | 默认值 |")
                md_content.append("|-------|------|---------|------|------|--------|")

                for prop in schema.properties:
                    required = "是" if prop.required else "否"
                    example = json.dumps(prop.example) if prop.example else ""
                    default = json.dumps(prop.default) if prop.default else ""
                    md_content.append(
                        f"| {prop.name} | {prop.prop_type} | {required} | {prop.description} | {example} | {default} |")

                md_content.append("")

            if schema.example:
                example_str = json.dumps(schema.example, ensure_ascii=False, indent=2)
                md_content.append(f"**完整示例**: \n```json\n{example_str}\n```")
                md_content.append("")

            md_content.append("---")
            md_content.append("")

        # 错误处理说明
        md_content.append("## 错误处理")
        md_content.append("")
        md_content.append("通用响应格式:")
        md_content.append("```json")
        md_content.append('{')
        md_content.append('  "code": 0, // 错误码')
        md_content.append('  "errCode": 0, // 详细错误码')
        md_content.append('  "message": "success", // 信息说明')
        md_content.append('  "data": {} // 返回数据')
        md_content.append('}')
        md_content.append("```")
        md_content.append("")

        md_content.append("常见错误码:")
        md_content.append("- `0`: 成功")
        md_content.append("- `1030400`: 错误请求 (BadRequest)")
        md_content.append("- `1030401`: 未授权 (Unauthorized)")
        md_content.append("- `1030403`: 禁止访问 (Forbidden)")
        md_content.append("- `1030404`: 未找到 (NotFound)")
        md_content.append("- `1030500`: 服务器内部错误 (InternalServerError)")
        md_content.append("")

        # 最佳实践
        md_content.append("## 最佳实践")
        md_content.append("")
        md_content.append("1. **认证**: 所有需要认证的接口请在请求头中添加 `authorization: Bearer <token>`")
        md_content.append("2. **时区处理**: 对于时间敏感的操作，始终传递时区信息")
        md_content.append("3. **分页**: 列表接口支持 `$skip` 和 `$limit` 参数进行分页")
        md_content.append("4. **错误处理**: 检查响应中的 `code` 字段，非0值表示操作失败")
        md_content.append("5. **数据验证**: 客户端应对输入数据进行基本验证，减少无效请求")
        md_content.append("")

        # 总结
        md_content.append("## 总结")
        md_content.append("")
        md_content.append(
            "本API文档提供了vivalink医疗健康监测平台的完整接口说明，包括用户认证、患者管理、心电数据、血压数据、PRO数据等核心功能。所有接口均采用RESTful设计风格，使用JSON格式进行数据交换，并支持完善的错误处理和时区管理。")
        md_content.append("")
        md_content.append(
            "开发者在集成时应特别注意时区处理和认证机制，确保数据的准确性和安全性。如有任何疑问，请参考具体接口的详细说明或联系技术支持。")
        md_content.append("")
        md_content.append(f"*文档生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

        # 将内容连接成字符串
        markdown_output = "\n".join(md_content)

        # 如果需要保存到文件
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_output)
            print(f"文档已保存到: {output_file}")

        return markdown_output


def main():
    # 使用示例
    parser = SwaggerParser("swagger_20250909.yaml")
    parser.load_swagger_file()

    # 生成Markdown文档
    markdown_doc = parser.generate_markdown_documentation("api_documentation.md")

    # 打印部分内容预览
    print(markdown_doc[:1000] + "...")

    # 为特定接口生成详细示例
    auth_endpoint = next((e for e in parser.endpoints if e.path == '/authentication' and e.method == 'POST'), None)
    if auth_endpoint:
        print("\n=== 认证接口详细示例 ===")
        print("\n请求示例:")
        print(parser.generate_request_example(auth_endpoint))

        print("\n成功响应示例:")
        print(parser.generate_response_example(auth_endpoint, "200"))

        print("\n错误响应示例:")
        print(parser.generate_error_response_example(auth_endpoint))


if __name__ == "__main__":
    main()
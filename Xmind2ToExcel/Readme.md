# Xmind To Excel

本工具可将Xmind编写的测试用例导出成Excel文件，但Xmind 测试用例必须按照特定的规范进行编写，具体规范如下。



## 设计/规则

1. 根节点将作为excel 的文件名。
2. Case最多4级模块，用1,2,3,4进行标记，若整个文件中存在case达到4级模块，那么导出的Excel中将自动生成包含4级模块的模板，其他没有4级模块的case将留空白...Excel生成的模块数取决于Xmind中最大的模块。
3. Case 需要必须包含标题，步骤，预期（前置可无）。
4. Case 可以用"勾"和"叹号" 来进行标记，"勾"代表 PASS，"叹号"表示FAIL。
5. FAIL的Case可以使用"笔记" / "标签" 两种方式进行描述，这部分将在Excel  Note列中进行展示。"笔记"/"标签"请使用一种方式进行备注。
6. 节点中包含"标题"的表示Case，若节点既没有标记也不属于Case，这样的节点将会被过滤掉。
7. 在Case的节点上标记 1,2,3表示该用例需要进入到回归测试中且对该回归测试的用例标记优先级。

## 工具打包
windows 使用 pyinstaller
mac 使用 py2app  命令： python3 setup.py py2app -A




Pyinstaller -F -w main.py -n "Xmind2ToExcel v1.1.0.0"  --distpath=D:\01-WorkSpace\01-Code\01-Python\可执行文件 --workpath=D:\01-WorkSpace\01-Code\01-Python\可执行文件\workpath --version-file=.\version_info.txt







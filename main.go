package main

import "github.com/sfx1117/easySwap-sync/cmd"

/*
*
1. 解析 import 的包并执行它们的 init()
2. 执行 cmd 包的 init() [按文件顺序]
  - 第一个 init(): 注册 initConfig 钩子 + 定义 flag
  - 第二个 init(): 注册 DaemonCmd 子命令

3. 执行 main()
  - 调用 cmd.Execute()

4. Cobra 内部流程:
  - 调用所有 OnInitialize() 注册的函数（执行 initConfig）
  - 解析命令行参数
  - 执行匹配命令的 Run 函数
*/
func main() {
	cmd.Execute() //执行命令
}

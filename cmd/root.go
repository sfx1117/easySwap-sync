package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)
import "github.com/spf13/viper"
import "github.com/mitchellh/go-homedir"

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "sync",
	Short: "root server.",
	Long:  "root server.",
}

// 在main函数之前执行
func init() {
	cobra.OnInitialize(initConfig)     // 注册初始化钩子
	flags := rootCmd.PersistentFlags() //持久化标志，所有子命令都可以使用这个标志
	flags.StringVarP(&cfgFile, "config", "c", "./config/config_import.toml", "config file (default is $HOME/.config_import.toml)")
}

// 初始化配置，viper配置中心
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile) // 指定配置文件
	} else {
		//获取根目录路径
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		viper.AddConfigPath(home)            // 添加配置路径
		viper.SetConfigName("config_import") // 配置文件名称（无扩展名）
	}
	viper.AutomaticEnv()           // 读取环境变量
	viper.SetConfigType("toml")    // 设置配置文件类型为toml
	viper.SetEnvPrefix("EasySwap") // 设置环境变量前缀
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		panic(err)
	}
}

// 执行命令
func Execute() {
	err := rootCmd.Execute() //执行主命令和子命令
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("cfgFile=", cfgFile)
}

package cmd

import (
	"context"
	"fmt"
	"github.com/ProjectsTask/EasySwapBase/logger/xzap"
	"github.com/sfx1117/easySwap-sync/src/config"
	"github.com/sfx1117/easySwap-sync/src/service"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

/*
*
定义daemon子命令
*/
var DaemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "sync easy swap order info.",
	Long:  "sync easy swap order info.",
	Run: func(cmd *cobra.Command, args []string) {
		var wg sync.WaitGroup
		wg.Add(1)

		ctx := context.Background()            // 创建根上下文
		ctx, cancel := context.WithCancel(ctx) // 创建可取消的派生上下文，用于控制 goroutine
		onSyncExit := make(chan error, 1)      // rpc退出信号通知chan

		go func() {
			defer wg.Done()
			//1、解析配置文件
			config, err := config.UnmarshalCmdConfig()
			if err != nil {
				//打印日志
				xzap.WithContext(ctx).Error("Failed to unmarshal config", zap.Error(err))
				//向退出信号通道中 发送错误信息
				onSyncExit <- err
				return
			}

			//2、初始化日志模块
			_, err = xzap.SetUp(*config.Log)
			if err != nil {
				xzap.WithContext(ctx).Error("Failed to set up logger", zap.Error(err))
				onSyncExit <- err
				return
			}

			//3、start server 打印日志
			xzap.WithContext(ctx).Info("sync start server", zap.Any("config", config))

			//4、初始化服务
			s, err := service.New(ctx, config)
			if err != nil {
				xzap.WithContext(ctx).Error("Failed to create sync server", zap.Error(err))
				onSyncExit <- err
				return
			}

			//5、启动服务
			err = s.Start()
			if err != nil {
				xzap.WithContext(ctx).Error("Failed to start sync server", zap.Error(err))
				onSyncExit <- err
				return
			}

			//6、开启监视器，用于性能监控
			if config.Monitor.PprofEnable {
				http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", config.Monitor.PprofPort), nil)
			}
		}()

		//信号通知chan
		onSignal := make(chan os.Signal, 1)
		//signal.Notify：注册要监听的信号类型：SIGINT：终端中断; SIGTERM：终止信号
		signal.Notify(onSignal, syscall.SIGINT, syscall.SIGTERM)
		//多路选择器
		select {
		case sig := <-onSignal: //外部信号触发
			switch sig {
			case syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM:
				cancel()
				xzap.WithContext(ctx).Info("Exit by signal", zap.String("signal", sig.String()))
			}
		case err := <-onSyncExit: //内部错误触发
			cancel()
			xzap.WithContext(ctx).Error("Exit by error", zap.Error(err))
		}
		wg.Wait()
	},
}

func init() {
	// 注册子命令
	rootCmd.AddCommand(DaemonCmd)
}

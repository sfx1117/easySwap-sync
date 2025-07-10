package model

import (
	"context"
	"github.com/ProjectsTask/EasySwapBase/stores/gdb"
	"gorm.io/gorm"
)

// 数据库初始化
func NewDB(ndb *gdb.Config) *gorm.DB {
	db := gdb.MustNewDB(ndb)    // 创建数据库连接
	ctx := context.Background() // 创建上下文
	err := InitModel(ctx, db)   // 初始化模型
	if err != nil {
		panic(err)
	}
	return db // 返回数据库连接实例
}

// 初始化模型
func InitModel(ctx context.Context, db *gorm.DB) error {
	//设置GORM的全局表选项
	//存储引擎：InnoDB
	//自增起始值：1
	//字符集：utf8mb4（支持完整Unicode，包括emoji）
	//排序规则：utf8mb4_general_ci
	err := db.Set(
		"gorm:table_options",
		"ENGINE=InnoDB AUTO_INCREMENT=1 CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci").Error
	if err != nil {
		return err
	}
	return nil
}

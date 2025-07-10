package filter

import (
	"context"
	"github.com/ProjectsTask/EasySwapBase/stores/gdb"
	"github.com/pkg/errors"
	"github.com/sfx1117/easySwap-sync/src/comm"
	"gorm.io/gorm"
	"strings"
	"sync"
)

// 集合过滤器
type CollectionFilter struct {
	ctx     context.Context
	db      *gorm.DB
	chain   string
	set     map[string]bool
	lock    *sync.RWMutex //读写锁
	project string
}

func NewCollectionFilter(ctx context.Context, db *gorm.DB, chain string, project string) *CollectionFilter {
	return &CollectionFilter{
		ctx:     ctx,
		db:      db,
		chain:   chain,
		set:     make(map[string]bool),
		lock:    &sync.RWMutex{},
		project: project,
	}
}

// 向过滤器中添加元素
func (cf *CollectionFilter) Add(element string) {
	cf.lock.Lock()
	defer cf.lock.Unlock()
	cf.set[strings.ToLower(element)] = true
}

// 从过滤器中移除元素
func (cf *CollectionFilter) Remove(element string) {
	cf.lock.Lock()
	defer cf.lock.Unlock()
	delete(cf.set, strings.ToLower(element))
}

// 检查元素是否存在于过滤器中
func (cf *CollectionFilter) Exist(element string) bool {
	cf.lock.Lock()
	defer cf.lock.Unlock()
	_, exist := cf.set[strings.ToLower(element)]
	return exist
}

// 从数据库预加载集合地址到过滤器中
func (cf *CollectionFilter) PreloadCollections() error {
	var address []string
	var err error

	err = cf.db.WithContext(cf.ctx).
		Table(gdb.GetMultiProjectCollectionTableName(cf.project, cf.chain)).
		Select("address").
		Where("floor_price_status = ?", comm.CollectionFloorPriceImported).
		Scan(&address).Error
	if err != nil {
		return errors.Wrap(err, "failed on query collections from db")
	}
	for _, v := range address {
		cf.Add(v)
	}
	return nil
}

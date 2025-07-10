package service

import (
	"context"
	"fmt"
	"github.com/ProjectsTask/EasySwapBase/chain"
	"github.com/ProjectsTask/EasySwapBase/chain/chainclient"
	"github.com/ProjectsTask/EasySwapBase/ordermanager"
	"github.com/ProjectsTask/EasySwapBase/stores/xkv"
	"github.com/pkg/errors"
	"github.com/sfx1117/easySwap-sync/src/config"
	"github.com/sfx1117/easySwap-sync/src/filter"
	"github.com/sfx1117/easySwap-sync/src/model"
	"github.com/sfx1117/easySwap-sync/src/orderbookindexer"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/kv"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
	"sync"
)

type Service struct {
	ctx              context.Context
	config           *config.Config
	kvStore          *xkv.Store
	db               *gorm.DB
	wg               *sync.WaitGroup
	collectionFilter *filter.CollectionFilter
	orderbookIndexer *orderbookindexer.Service
	orderManager     *ordermanager.OrderManager
}

func New(ctx context.Context, config *config.Config) (*Service, error) {
	//1、初始化redis/kv存储
	var kvConf kv.KvConf
	for _, con := range config.Kv.Redis {
		kvConf = append(kvConf, cache.NodeConf{
			RedisConf: redis.RedisConf{
				Host: con.Host,
				Pass: con.Pass,
				Type: con.Type,
			},
			Weight: 2,
		})
	}
	kvstore := xkv.NewStore(kvConf)

	//2、初始化数据库
	db := model.NewDB(config.DB)

	//3、初始化集合过滤器
	collectionFilter := filter.NewCollectionFilter(ctx, db, config.ChainCfg.Name, config.ProjectCfg.Name)

	//4、初始化ordermanager
	orderManager := ordermanager.New(ctx, db, kvstore, config.ChainCfg.Name, config.ProjectCfg.Name)

	//5、初始化chainClient
	fmt.Println("chainClient url :" + config.AnkrCfg.HttpsUrl + config.AnkrCfg.ApiKey)
	chainClient, err := chainclient.New(int(config.ChainCfg.ID), config.AnkrCfg.HttpsUrl+config.AnkrCfg.ApiKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed on create evm client")
	}

	//6、初始化orderbookSyncer
	var orderbookSyncer *orderbookindexer.Service
	if config.ChainCfg.ID == chain.EthChainID ||
		config.ChainCfg.ID == chain.OptimismChainID ||
		config.ChainCfg.ID == chain.SepoliaChainID {
		orderbookSyncer = orderbookindexer.New(ctx, config, db, kvstore, chainClient, config.ChainCfg.ID, config.ChainCfg.Name, orderManager)
	}

	//7、
	manager := Service{
		ctx:              ctx,
		config:           config,
		kvStore:          kvstore,
		db:               db,
		wg:               &sync.WaitGroup{},
		collectionFilter: collectionFilter,
		orderbookIndexer: orderbookSyncer,
		orderManager:     orderManager,
	}
	return &manager, nil
}

// Start 启动服务
func (s *Service) Start() error {
	err := s.collectionFilter.PreloadCollections()
	if err != nil {
		return errors.Wrap(err, "failed on preload collection to filter")
	}
	s.orderbookIndexer.Start()
	s.orderManager.Start()
	return nil
}

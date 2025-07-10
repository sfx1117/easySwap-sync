package config

import (
	logging "github.com/ProjectsTask/EasySwapBase/logger"
	"github.com/ProjectsTask/EasySwapBase/stores/gdb"
	"github.com/spf13/viper"
)

type Config struct {
	Monitor     *Monitor         `toml:"monitor" mapstructure:"monitor" json:"monitor"`
	Log         *logging.LogConf `toml:"log" mapstructure:"log" json:"log"`
	Kv          *KvConf          `toml:"kv" mapstructure:"kv" json:"kv"`
	DB          *gdb.Config      `toml:"db" mapstructure:"db" json:"db"`
	AnkrCfg     AnkrCfg          `toml:"ankr_cfg" mapstructure:"ankr_cfg" json:"ankr_cfg"`
	ChainCfg    ChainCfg         `toml:"chain_cfg" mapstructure:"chain_cfg" json:"chain_cfg"`
	ContractCfg ContractCfg      `toml:"contract_cfg" mapstructure:"contract_cfg" json:"contract_cfg"`
	ProjectCfg  ProjectCfg       `toml:"project_cfg" mapstructure:"project_cfg" json:"project_cfg"`
}

// 监视器配置类
type Monitor struct {
	PprofEnable bool  `toml:"pprof_enable" mapstructure:"pprof_enable" json:"pprof_enable"`
	PprofPort   int64 `toml:"pprof_port" mapstructure:"pprof_port" json:"pprof_port"`
}

// redis配置类
type KvConf struct {
	Redis []*Redis `toml:"redis" json:"redis"`
}
type Redis struct {
	Host string `toml:"host" json:"host"`
	Type string `toml:"type" json:"type"`
	Pass string `toml:"pass" json:"pass"`
}

// 第三方api接口服务配置类
type AnkrCfg struct {
	ApiKey       string `toml:"api_key" mapstructure:"api_key" json:"api_key"`
	HttpsUrl     string `toml:"https_url" mapstructure:"https_url" json:"https_url"`
	WebsocketUrl string `toml:"websocket_url" mapstructure:"websocket_url" json:"websocket_url"`
	EnableWss    bool   `toml:"enable_wss" mapstructure:"enable_wss" json:"enable_wss"`
}

// 链配置类
type ChainCfg struct {
	Name string `toml:"name" mapstructure:"name" json:"name"`
	ID   int64  `toml:"id" mapstructure:"id" json:"id"`
}

// 合约配置类
type ContractCfg struct {
	EthAddress  string `toml:"eth_address" mapstructure:"eth_address" json:"eth_address"`
	WethAddress string `toml:"weth_address" mapstructure:"weth_address" json:"weth_address"`
	DexAddress  string `toml:"dex_address" mapstructure:"dex_address" json:"dex_address"`
}

// 项目配置类
type ProjectCfg struct {
	Name string `toml:"name" mapstructure:"name" json:"name"`
}

// 将配置文件解析到Config
func UnmarshalCmdConfig() (*Config, error) {
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var c Config
	err = viper.Unmarshal(&c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

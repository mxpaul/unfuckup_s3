package cmd

import (
	//"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	//yaml "gopkg.in/yaml.v2"
)

var (
	// Used for flags.
	cfgFile string

	rootCmd = &cobra.Command{
		Use:   "unfuckup",
		Short: "Fix different fuckups",
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfigOrDie)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "unfuckup.yaml", "config file (default unfuckup.yaml)")

	s3Cmd := &cobra.Command{
		Use:   "s3",
		Short: "unfuckup s3 deleted files",
		Long: `Пример реализации тестового задания с восстановлением удаленных файлов в s3 из бекапа

Есть файл со списком id-файлов в amazon s3 хранилище, в файле 5Млн строк.
Все файлы были удалены системным администратором по ошибке.
Страдают живые пользователи, не могут скачать свои личные файлы.

Требуется восстановить этот список файлов максимально быстро, насколько это возможно.
Нужно написать программу которая восстановит все эти файлы.
Каждый файл доступен по бекап-урлу https://cloud.i/backup/<file-id>/

s3 эмулируется походом в заданный конфигом URL через PUT запрос

`,
		Run: s3Run,
	}

	s3Cmd.PersistentFlags().StringP("input", "i", defaultInputFile, "input file, list of deleted id, one file id per line")
	s3Cmd.PersistentFlags().Uint64("offset", defaultOffset, "skip this number of file ids in input file")
	s3Cmd.PersistentFlags().Uint64("limit", defaultLimit, "stop parsing input file after processing this number of lines")

	if err := viper.BindPFlag("s3.input", s3Cmd.PersistentFlags().Lookup("input")); err != nil {
		log.Fatalf("BindPFlag s3.input error: %s", err)
	}
	if err := viper.BindPFlag("s3.generator.offset", s3Cmd.PersistentFlags().Lookup("offset")); err != nil {
		log.Fatalf("BindPFlag s3.generator.offset error: %s", err)
	}
	if err := viper.BindPFlag("s3.generator.limit", s3Cmd.PersistentFlags().Lookup("limit")); err != nil {
		log.Fatalf("BindPFlag s3.generator.limit error: %s", err)
	}

	viper.SetDefault("s3.input", defaultInputFile)
	viper.SetDefault("s3.generator.offset", defaultOffset)
	viper.SetDefault("s3.generator.limit", defaultLimit)
	viper.SetDefault("s3.generator.value_channel_capacity", defaultValueChannelCapacity)
	viper.SetDefault("s3.generator.error_channel_capacity", defaultErrorChannelCapacity)

	rootCmd.AddCommand(s3Cmd)

	//rootCmd.AddCommand(&cobra.Command{
	//	Use:   "printconfig",
	//	Short: "print current effective config in yaml format",
	//	Run: func(cmd *cobra.Command, args []string) {
	//		c := viper.AllSettings()
	//		bs, err := yaml.Marshal(c)
	//		if err != nil {
	//			log.Fatalf("unable to marshal config to YAML: %v", err)
	//		}
	//		fmt.Print(string(bs))
	//	},
	//})
}

func initConfigOrDie() {
	viper.SetConfigFile(cfgFile)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf(`Config file "%s" error: %s`, viper.ConfigFileUsed(), err)
	}
	// FIXME: check for required sections presence
	log.Printf("Using config file: %s", viper.ConfigFileUsed())
}

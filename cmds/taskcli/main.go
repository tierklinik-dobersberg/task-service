package main

import (
	"github.com/sirupsen/logrus"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/task-service/cmds/taskcli/cmds"
)

func main() {
	root := cli.New("taskcli")

	root.AddCommand(
		cmds.GetBoardsCommand(root),
	)

	if err := root.Execute(); err != nil {
		logrus.Fatal(err.Error())
	}
}

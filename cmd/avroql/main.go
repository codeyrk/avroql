package main

import (
	"os"

	avroql "github.com/codeyrk/avroql"
	"github.com/sirupsen/logrus"
	sql "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/server"
)

func main() {
	dir := "."
	if len(os.Args) >= 2 {
		dir = os.Args[1]
	}
	e := sql.NewDefault()
	d, err := avroql.NewDatabase(dir)
	if err != nil {
		logrus.Fatalf("could not create database: %v", err)
	}
	e.AddDatabase(d)

	if err := e.Init(); err != nil {
		logrus.Fatalf("could not initialize server: %v", err)
	}

	cfg := server.Config{
		Protocol: "tcp",
		Address:  "0.0.0.0:3306",
		Auth:     auth.NewNativeSingle("root", "root", auth.AllPermissions),
	}
	s, err := server.NewDefaultServer(cfg, e)
	if err != nil {
		logrus.Fatalf("could not create default server: %v", err)
	}

	logrus.Infof("server started on %s", cfg.Address)
	if err := s.Start(); err != nil {
		logrus.Fatalf("server failed: %v", err)
	}
}

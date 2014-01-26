package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/mortdeus/go9p"
	"github.com/mortdeus/go9p/srv"
	"labix.org/v2/mgo"
	"korvin/mongofs/collections"
)

var addr = flag.String("addr", ":27117", "network address")
var mongo = flag.String("mongo", "localhost", "mongodb address")
var dbname = flag.String("dbname", "test", "database name")

func main() {
	flag.Parse()
	if err := start(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func start() error {
	session, err := mgo.Dial(*mongo)
	if err != nil {
		return err
	}
	defer session.Close()
	user := go9p.OsUsers.Uid2User(os.Geteuid())
	root := new(srv.File)
	if err := root.Add(nil, "/", user, nil, go9p.DMDIR|0555, nil); err != nil {
		return err
	}
	if err := collections.Add(root, user, session.DB(*dbname)); err != nil {
		return err
	}
	s := srv.NewFileSrv(root)
	s.Dotu = true
	s.Start(s)
	//s.Debuglevel = srv.DbgPrintFcalls
	return s.StartNetListener("tcp", *addr)
}
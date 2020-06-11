package mys3

import "fmt"

func BuildMySQLDSN(dbUser, dbPassword, dbHost, dbPort, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
}

package mysql

import "testing"

func TestName(t *testing.T) {
	Init("root:111@tcp(127.0.0.1:3306)/go_test")

}

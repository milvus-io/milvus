package goplugin

import (
	"fmt"
)

var msg Msg

type Msg struct {
	num int
}

func HelloWorld() {
	fmt.Println("Hello World")
}

func Add(a, b int) int {
	return a + b
}

func AddOnce() {
	msg.num++
}

func GetNum() int {
	return msg.num
}

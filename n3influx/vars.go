package n3influx

import (
	"fmt"
	"strings"

	u "github.com/cdutwhu/go-util"
)

var (
	uPE  = u.PanicOnError
	uPE1 = u.PanicOnError1
	uPH  = u.PanicHandle
	uPC  = u.PanicOnCondition

	fPf  = fmt.Printf
	fSpf = fmt.Sprintf
	fPln = fmt.Println

	sI = strings.Index
)

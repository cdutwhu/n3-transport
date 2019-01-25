package n3node

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

	fPln = fmt.Println
	fPf  = fmt.Printf
	fSpf = fmt.Sprintf

	sHP = strings.HasPrefix
	sHS = strings.HasSuffix
)

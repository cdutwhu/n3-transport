package n3node

import (
	"fmt"
	"strings"

	u "github.com/cdutwhu/go-util"
)

var (
	PE   = u.PanicOnError
	PE1  = u.PanicOnError1
	PH   = u.PanicHandle
	PC   = u.PanicOnCondition
	Must = u.Must

	fPln = fmt.Println
	fPf  = fmt.Printf
	fSf  = fmt.Sprintf

	sHP  = strings.HasPrefix
	sHS  = strings.HasSuffix
	sSpl = strings.Split

	prevID   = ""
	prevPred = ""
	prevVer  int64
	startVer int64
	verMeta  int64 = 1
	mapIDVQ        = make(map[string][]int64)
)

const (
	TERMMARK = "ENDENDEND"
)

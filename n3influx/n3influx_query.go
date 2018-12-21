package n3influx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	u "github.com/cdutwhu/util"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/nsip/n3-messages/messages/pb"
)

var (
	pf, spf, pln = fmt.Printf, fmt.Sprintf, fmt.Println
)

// SubExist :
func (n3ic *Publisher) SubExist(tuple *pb.SPOTuple, contextName, db string) bool {
	// pln("checking subject ...")
	qStr := spf("SELECT object, version FROM %s WHERE subject = '%s' ORDER BY time DESC LIMIT 1", contextName, tuple.Subject)
	resp, err := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	PE(err)
	PE(resp.Error())
	return len(resp.Results[0].Series) != 0
}

// GetObj :
func (n3ic *Publisher) GetObj(tuple *pb.SPOTuple, offset int, isArray bool, contextName, db string) (string, int64, bool) {
	// pln("looking for object ...")
	// if !isArray {
	subject, predicate := tuple.Subject, tuple.Predicate
	qStr := spf("SELECT object, version FROM %s WHERE subject = '%s' AND predicate = '%s' ORDER BY time DESC LIMIT 1 OFFSET %d", contextName, subject, predicate, offset)
	resp, err := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	PE(err)
	PE(resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		// pln(resp.Results[0].Series[0].Values)
		// pln(resp.Results[0].Series[0].Values[0])
		// pln(resp.Results[0].Series[0].Values[0][1]) /* [0] is time, [1] is object, as select ... */
		ver, err := resp.Results[0].Series[0].Values[0][2].(json.Number).Int64()
		PE(err)
		return resp.Results[0].Series[0].Values[0][1].(string), ver, true
	}
	// } else {
	// 	pf(" ******************************************* %s No. %d \n", tuple.Predicate, offset)
	// }
	return "", -1, false
}

// GetSubStruct :
func (n3ic *Publisher) GetSubStruct(tuple *pb.SPOTuple, contextName, db string) (string, bool) {
	// pln("looking for sub struct ...")

	subject, predicate := u.Str(tuple.Predicate).RemovePrefix("sif."), "::"
	qStr := spf("SELECT object, version FROM %s WHERE subject = '%s' AND predicate = '%s' ORDER BY time DESC LIMIT 1", contextName, subject, predicate)
	resp, err := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	PE(err)
	PE(resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		return resp.Results[0].Series[0].Values[0][1].(string), true
	}
	return "", false
}

// QueryTuple :
func (n3ic *Publisher) QueryTuple(tuple *pb.SPOTuple, offset int, isArray bool, contextName string, ts *[]*pb.SPOTuple, arrInfo *map[string]int) {
	db := "tuples"

	if !n3ic.SubExist(tuple, contextName, db) {
		pln("subject does not exist !")
		return
	}

	if obj, ver, ok := n3ic.GetObj(tuple, offset, isArray, contextName, db); ok {
		// pf("got object .................. %d \n", ver)
		tuple.Object = obj
		*ts = append(*ts, &pb.SPOTuple{
			Subject:   tuple.Subject, // + spf(" # %d", ver),
			Predicate: tuple.Predicate,
			Object:    obj,
			Version:   ver,
		})
		return
	}

	if stru, ok := n3ic.GetSubStruct(tuple, contextName, db); ok {
		// pln(spf("got sub-struct, more work to do ......"))

		/* Array Element */
		if i, j := strings.Index(stru, "["), strings.Index(stru, "]"); i == 0 && j > 1 {
			nArr, _ := strconv.Atoi(stru[1:j])
			// pln(nArr)
			s := stru[j+1:]
			tuple.Predicate += spf(".%s", s)
			// pln(tuple.Predicate)

			(*arrInfo)[tuple.Predicate] = nArr /* keep the array */

			for k := 0; k < nArr; k++ {
				n3ic.QueryTuple(tuple, nArr-k-1, true, contextName, ts, arrInfo)
			}
			tuple.Predicate = u.Str(tuple.Predicate).RemoveTailFromLast(".")
			return
		}

		/* None Array Element */
		subs := strings.Split(stru, " + ")
		for _, s := range subs {
			tuple.Predicate += spf(".%s", s)
			// pln(tuple.Predicate)
			n3ic.QueryTuple(tuple, offset, false, contextName, ts, arrInfo)
			tuple.Predicate = u.Str(tuple.Predicate).RemoveTailFromLast(".")
		}
		return

		/* Only for debugging */
		// tuple.Object = stru
		// *ts = append(*ts, tuple)
		// return
	}
}

func getVersByPre(ts *[]*pb.SPOTuple, pre string) (vers []int64, indices []int) {
	for i, t := range *ts {
		if t.Predicate == pre {
			vers = append(vers, t.Version)
			indices = append(indices, i)
		}
	}
	return
}

// func moveTupleAfterVer(ts *[]*pb.SPOTuple, tIn *pb.SPOTuple, ver int64) {
// 	bFindVer, bDel := false, false
// 	for _, t := range *ts {
// 		if t.Version == ver {
// 			bFindVer = true
// 			break
// 		}
// 	}
// 	if bFindVer {
// 		for i, t := range *ts {
// 			if t == tIn {
// 				*ts = append((*ts)[:i], (*ts)[i+1:]...)
// 				bDel = true
// 				break
// 			}
// 		}
// 		if bDel {
// 			for i, t := range *ts {
// 				if t.Version == ver {
// 					*ts = append(*ts, nil)
// 					copy((*ts)[i+2:], (*ts)[i+1:])
// 					(*ts)[i+1] = tIn
// 					break
// 				}
// 			}
// 		}
// 	}
// }

type arrOptTupleInfo struct {
	nArray   int     /* 'this' tuple's array's count */
	nTuple   int     /* 'this' tuple count in real */
	indices  []int   /* optional tuple's index in ts */
	versions []int64 /* optional tuple's version */
}

// AdjustOptionalTuples : according to tuple version, adjust some tuples' order in sub array tuple
func (n3ic *Publisher) AdjustOptionalTuples(ts *[]*pb.SPOTuple, arrInfo *map[string]int) {
	mapTuplesInArr := make(map[string]*arrOptTupleInfo)
	for ituple, tuple := range *ts {
		pre, ver := tuple.Predicate, tuple.Version
		if ok, nArr := u.Str(pre).CoverAnyKeyInMapSI(*arrInfo); ok {
			pf("%d : %s : %d # %d\n", ituple, pre, nArr, ver)
			if _, ok := mapTuplesInArr[pre]; !ok {
				mapTuplesInArr[pre] = &arrOptTupleInfo{}
			}
			mapTuplesInArr[pre].nArray = nArr
			mapTuplesInArr[pre].nTuple++
			mapTuplesInArr[pre].indices = append(mapTuplesInArr[pre].indices, ituple)
			mapTuplesInArr[pre].versions = append(mapTuplesInArr[pre].versions, ver)
		}
	}

	mapOptTuple := make(map[string]*arrOptTupleInfo)
	for k, v := range mapTuplesInArr {
		if v.nArray != v.nTuple {
			mapOptTuple[k] = v
		}
	}

	optTuples, aboveTuples := []*pb.SPOTuple{}, []*pb.SPOTuple{}
	for _, v := range mapOptTuple {
		for i, tIdx := range v.indices {
			this, above := (*ts)[tIdx], (*ts)[tIdx-1]
			pf("%d --- %v\n", tIdx, this)
			if vers, indicesTS := getVersByPre(ts, above.Predicate); len(vers) > 0 {
				tVer := v.versions[i]
				_, idx := u.I64(tVer).Nearest(vers...)
				posShouldBeAfter := indicesTS[idx]
				shouldBeAfter := (*ts)[posShouldBeAfter]
				pf("Should be after : %d --- %v\n", posShouldBeAfter, shouldBeAfter)
				optTuples, aboveTuples = append(optTuples, this), append(aboveTuples, shouldBeAfter)
			}
		}
	}

	/******************************************/

	tempts := make([]interface{}, len(*ts))
	for j, t := range *ts {
		tempts[j] = t
	}
	for i := 0; i < len(optTuples); i++ {
		// moveTupleAfterVer(ts, optTuples[i], aboveTuples[i].Version)
		u.MoveItemAfter(&tempts, func(after, move interface{}) bool {
			return after == aboveTuples[i] && move == optTuples[i]
		})
	}
	for k, t := range tempts {
		(*ts)[k] = t.(*pb.SPOTuple)
	}
}

package n3influx

import (
	"encoding/json"
	"strconv"
	"strings"

	u "github.com/cdutwhu/go-util"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/nsip/n3-messages/messages/pb"
)

// SubExist :
func (n3ic *Publisher) SubExist(tuple *pb.SPOTuple, ctxName, db string) bool {
	// pln("checking subject ...")
	qStr := fSpf(`SELECT object, version FROM "%s" WHERE subject = '%s' ORDER BY time DESC LIMIT 1`, ctxName, tuple.Subject)
	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e)
	uPE(resp.Error())
	return len(resp.Results[0].Series) != 0
}

// GetObjs (for XAPI query) : (return objects, versions, IsFound)
func (n3ic *Publisher) GetObjs(tuple *pb.SPOTuple, ctxName, db string) (preds, objs []string, vers []int64, found bool) {

	subject, predicate := tuple.Subject, tuple.Predicate
	qStr := fSpf(`SELECT predicate, object, version FROM "%s" WHERE subject = '%s' AND predicate =~ /^%s\./ ORDER BY time DESC`, ctxName, subject, predicate)
	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e)
	uPE(resp.Error())

	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		for _, l := range resp.Results[0].Series[0].Values {
			pred, obj := l[1].(string), l[2].(string)
			preds, objs = append(preds, pred), append(objs, obj)
			v, e := l[3].(json.Number).Int64()
			uPE(e)
			ver := v
			vers = append(vers, ver)
			fPln(pred, obj, ver)
		}
		found = true
		fPln()
		return
	}
	return
}

// GetObj : (return object, version, IsFound)
func (n3ic *Publisher) GetObj(tuple *pb.SPOTuple, offset int, isArray bool, ctxName, db string) (obj string, ver int64, found bool) {
	ver = -1

	// pln("looking for object ...")
	// if !isArray {
	subject, predicate := tuple.Subject, tuple.Predicate
	qStr := fSpf(`SELECT object, version FROM "%s" WHERE subject = '%s' AND predicate = '%s' ORDER BY time DESC LIMIT 1 OFFSET %d`, ctxName, subject, predicate, offset)
	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e)
	uPE(resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		// pln(resp.Results[0].Series[0].Values)
		// pln(resp.Results[0].Series[0].Values[0])
		// pln(resp.Results[0].Series[0].Values[0][1]) /* [0] is time, [1] is object, as select ... */
		obj = resp.Results[0].Series[0].Values[0][1].(string)
		v, e := resp.Results[0].Series[0].Values[0][2].(json.Number).Int64()
		uPE(e)
		ver = v
		found = true
		return
	}
	// } else {
	// 	pf(" ******************************************* %s No. %d \n", tuple.Predicate, offset)
	// }
	return
}

// GetSubStruct :
func (n3ic *Publisher) GetSubStruct(tuple *pb.SPOTuple, ctxName, db string) (string, bool) {
	// pln("looking for sub struct ...")

	subject, predicate := u.Str(tuple.Predicate).RemovePrefix("sif."), "::"
	qStr := fSpf("SELECT object, version FROM \"%s\" WHERE subject = '%s' AND predicate = '%s' ORDER BY time DESC LIMIT 1", ctxName, subject, predicate)
	resp, err := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(err)
	uPE(resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		return resp.Results[0].Series[0].Values[0][1].(string), true
	}
	return "", false
}

// QueryTuples (for XAPI query) :
func (n3ic *Publisher) QueryTuples(tuple *pb.SPOTuple, ctxName string, ts *[]*pb.SPOTuple) {
	db := "tuples"

	if !n3ic.SubExist(tuple, ctxName, db) {
		fPln("subject does not exist !")
		return
	}

	if preds, objs, vers, ok := n3ic.GetObjs(tuple, ctxName, db); ok {
		for i := range preds {
			*ts = append(*ts, &pb.SPOTuple{
				Subject:   tuple.Subject,
				Predicate: preds[i],
				Object:    objs[i],
				Version:   vers[i],
			})
		}
	}
}

// QueryTuple :
func (n3ic *Publisher) QueryTuple(tuple *pb.SPOTuple, offset int, isArray bool, ctxName string, ts *[]*pb.SPOTuple, arrInfo *map[string]int) {
	db := "tuples"

	if !n3ic.SubExist(tuple, ctxName, db) {
		fPln("subject does not exist !")
		return
	}

	if obj, ver, ok := n3ic.GetObj(tuple, offset, isArray, ctxName, db); ok {
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

	if stru, ok := n3ic.GetSubStruct(tuple, ctxName, db); ok {
		// pln(spf("got sub-struct, more work to do ......"))

		/* Array Element */
		if i, j := sI(stru, "["), sI(stru, "]"); i == 0 && j > 1 {
			nArr, _ := strconv.Atoi(stru[1:j])
			// pln(nArr)
			s := stru[j+1:]
			tuple.Predicate += fSpf(".%s", s)
			// pln(tuple.Predicate)

			(*arrInfo)[tuple.Predicate] = nArr /* keep the array */

			for k := 0; k < nArr; k++ {
				n3ic.QueryTuple(tuple, nArr-k-1, true, ctxName, ts, arrInfo)
			}
			tuple.Predicate = u.Str(tuple.Predicate).RemoveTailFromLast(".")
			return
		}

		/* None Array Element */
		subs := strings.Split(stru, " + ")
		for _, s := range subs {
			tuple.Predicate += fSpf(".%s", s)
			// pln(tuple.Predicate)
			n3ic.QueryTuple(tuple, offset, false, ctxName, ts, arrInfo)
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
			fPf("%d : %s : %d # %d\n", ituple, pre, nArr, ver)
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
			fPf("%d --- %v\n", tIdx, this)
			if vers, indicesTS := getVersByPre(ts, above.Predicate); len(vers) > 0 {
				tVer := v.versions[i]
				_, idx := u.I64(tVer).Nearest(vers...)
				posShouldBeAfter := indicesTS[idx]
				shouldBeAfter := (*ts)[posShouldBeAfter]
				fPf("Should be after : %d --- %v\n", posShouldBeAfter, shouldBeAfter)
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

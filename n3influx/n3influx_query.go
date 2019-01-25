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
func (n3ic *DBClient) SubExist(tuple *pb.SPOTuple, ctx string) bool {
	// pln("checking subject ...")
	qSelect := fSpf(`SELECT object, version FROM "%s" `, ctx)
	qWhere := fSpf(`WHERE subject='%s' AND version!=0 AND tombstone='false' `, tuple.Subject)
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC LIMIT 1`, orderByTm)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())
	return len(resp.Results[0].Series) != 0
}

// BatTransEx :
func (n3ic *DBClient) BatTransEx(tuple *pb.SPOTuple, ctx, ctxNew string, extSub, extPred bool,
	exclude func(s, p, o string, v int64) bool) (n int64) {

	if ss, ps, os, vs, ok := n3ic.GetObjs(tuple, ctx, extSub, extPred); ok {
		for i := range ss {
			if exclude(ss[i], ps[i], os[i], vs[i]) {
				continue
			}
			temp := &pb.SPOTuple{Subject: ss[i], Predicate: ps[i], Object: os[i], Version: vs[i]}
			uPE(n3ic.StoreTuple(temp, ctxNew))
			n++
		}
		// time.Sleep(500 * time.Millisecond)
	}
	return n
}

// BatTrans :
func (n3ic *DBClient) BatTrans(tuple *pb.SPOTuple, ctx, ctxNew string, extSub, extPred bool) int64 {
	subject, predicate := tuple.Subject, tuple.Predicate
	qSelect, qWhere := fSpf(`SELECT version, subject, predicate, object, tombstone INTO "%s" FROM "%s" `, ctxNew, ctx), ""
	if extSub && !extPred {
		qWhere = fSpf(`WHERE subject=~/^%s/ AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if extSub && extPred {
		qWhere = fSpf(`WHERE subject=~/^%s/ AND predicate=~/^%s/ AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if !extSub && extPred {
		qWhere = fSpf(`WHERE subject='%s' AND predicate=~/^%s/ AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if !extSub && !extPred {
		qWhere = fSpf(`WHERE subject='%s' AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	}
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC`, orderByTm)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())

	v, e := resp.Results[0].Series[0].Values[0][1].(json.Number).Int64()
	uPE(e)
	return v
}

// GetObjs (for XAPI query) : (return objects, versions, IsFound)
func (n3ic *DBClient) GetObjs(tuple *pb.SPOTuple, ctx string, extSub, extPred bool) (subs, preds, objs []string, vers []int64, found bool) {
	subject, predicate := tuple.Subject, tuple.Predicate

	qSelect, qWhere := fSpf(`SELECT subject, predicate, object, version FROM "%s" `, ctx), ""
	if extSub && !extPred {
		qWhere = fSpf(`WHERE subject=~/^%s/ AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if extSub && extPred {
		qWhere = fSpf(`WHERE subject=~/^%s/ AND predicate=~/^%s/ AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if !extSub && extPred {
		qWhere = fSpf(`WHERE subject='%s' AND predicate=~/^%s/ AND version!=0 AND tombstone='false' `, subject, predicate)
	} else if !extSub && !extPred {
		qWhere = fSpf(`WHERE subject='%s' AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	}
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC`, orderByTm)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())

	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		for _, l := range resp.Results[0].Series[0].Values {
			sub, pred, obj := l[1].(string), l[2].(string), l[3].(string)
			subs, preds, objs = append(subs, sub), append(preds, pred), append(objs, obj)
			v, e := l[4].(json.Number).Int64()
			uPE(e)
			ver := v
			vers = append(vers, ver)
			// fPln(pred, obj, ver)
		}
		found = true
	}
	return
}

// QueryTuples (for XAPI query) :
func (n3ic *DBClient) QueryTuples(tuple *pb.SPOTuple, ctx string, ts *[]*pb.SPOTuple) {
	if !n3ic.SubExist(tuple, ctx) {
		fPln("subject does not exist !")
		return
	}
	if subs, preds, objs, vers, ok := n3ic.GetObjs(tuple, ctx, false, true); ok {
		for i := range subs {
			*ts = append(*ts, &pb.SPOTuple{
				Subject:   subs[i],
				Predicate: preds[i],
				Object:    objs[i],
				Version:   vers[i],
			})
		}
	}
}

/**********************************************************************************************/

// GetVer :
func (n3ic *DBClient) GetVer(tuple *pb.SPOTuple, ctx string) int64 {
	if _, ver, found := n3ic.GetObj(tuple, 0, false, ctx); found {
		return ver
	}
	return -1
}

// GetObj : (return object, version, IsFound)
func (n3ic *DBClient) GetObj(tuple *pb.SPOTuple, offset int, isArr bool, ctx string) (obj string, ver int64, found bool) {
	ver = -1
	subject, predicate := tuple.Subject, tuple.Predicate
	// fPf("looking for object ... %s ... %s\n", subject, predicate)

	// if !isArr {
	qSelect := fSpf(`SELECT object, version FROM "%s" `, ctx)
	qWhere := fSpf(`WHERE subject='%s' AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC LIMIT 1 OFFSET %d`, orderByTm, offset)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		// pln(resp.Results[0].Series[0].Values)
		// pln(resp.Results[0].Series[0].Values[0])
		// pln(resp.Results[0].Series[0].Values[0][1]) /* [0] is time, [1] is object, as SELECT ... */

		obj = resp.Results[0].Series[0].Values[0][1].(string)
		v, e := resp.Results[0].Series[0].Values[0][2].(json.Number).Int64()
		uPE(e)
		ver = v
		found = true
		return
	}
	// } else {
	// 	pf(" ******************************************* %s No. %d \n", predicate, offset)
	// }
	return
}

// GetSubStruct :
func (n3ic *DBClient) getSubStruct(tuple *pb.SPOTuple, ctx string) (string, bool) {
	// pln("looking for sub struct ...")

	subject, predicate := tuple.Predicate, "::"

	qSelect := fSpf(`SELECT object, version FROM "%s" `, ctx)
	qWhere := fSpf(`WHERE subject='%s' AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC LIMIT 1`, orderByTm)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		return resp.Results[0].Series[0].Values[0][1].(string), true
	}
	return "", false
}

// GetArrInfo :
func (n3ic *DBClient) getArrInfo(tuple *pb.SPOTuple, ctx string) (int, bool) {
	// pln("looking for array info ...")

	subject, predicate := tuple.Predicate, tuple.Subject

	qSelect := fSpf(`SELECT object, version FROM "%s" `, ctx)
	qWhere := fSpf(`WHERE subject='%s' AND predicate='%s' AND version!=0 AND tombstone='false' `, subject, predicate)
	qStr := qSelect + qWhere + fSpf(`ORDER BY %s DESC LIMIT 1`, orderByTm)

	resp, e := n3ic.cl.Query(influx.NewQuery(qStr, db, ""))
	uPE(e, resp.Error())
	if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Values) > 0 {
		n, e := strconv.Atoi(resp.Results[0].Series[0].Values[0][1].(string))
		uPE(e)
		return n, true
	}
	return 0, false
}

// QueryTuple :
func (n3ic *DBClient) QueryTuple(tuple *pb.SPOTuple, offset int, isArr, revArr bool, ctx string, ts *[]*pb.SPOTuple, arrInfo *map[string]int) {
	if !n3ic.SubExist(tuple, ctx) {
		fPln("subject does not exist !")
		return
	}

	if obj, ver, ok := n3ic.GetObj(tuple, offset, isArr, ctx); ok {
		// fPf("got object .................. %d \n", ver)
		tuple.Object = obj
		*ts = append(*ts, &pb.SPOTuple{
			Subject:   tuple.Subject, // + spf(" # %d", ver),
			Predicate: tuple.Predicate,
			Object:    obj,
			Version:   ver,
		})
		return
	}

	if stru, ok := n3ic.getSubStruct(tuple, ctx); ok {
		// fPf("got sub-struct, more work ------> %s\n", stru)

		/* Array Element */
		if i := sI(stru, "[]"); i == 0 {
			nArr, _ := n3ic.getArrInfo(tuple, ctx)
			// pln(nArr)

			tuple.Predicate += fSpf(".%s", stru[i+2:])
			// pln(tuple.Predicate)

			(*arrInfo)[tuple.Predicate] = nArr /* keep the array */

			for k := 0; k < nArr; k++ {
				if revArr { /* If directly query from original table, use this */
					n3ic.QueryTuple(tuple, nArr-k-1, true, revArr, ctx, ts, arrInfo)
				} else { /* If regex query into a temp table in advance, then query from temp table, use this */
					n3ic.QueryTuple(tuple, k, true, revArr, ctx, ts, arrInfo)
				}
			}
			tuple.Predicate = u.Str(tuple.Predicate).RmTailFromLast(".")
			return
		}

		/* None Array Element */
		subs := strings.Split(stru, " + ")
		for _, s := range subs {
			tuple.Predicate += fSpf(".%s", s)
			// pln(tuple.Predicate)
			n3ic.QueryTuple(tuple, offset, false, revArr, ctx, ts, arrInfo)
			tuple.Predicate = u.Str(tuple.Predicate).RmTailFromLast(".")
		}
		return

		/* Only for debugging */
		// tuple.Object = stru
		// *ts = append(*ts, tuple)
		// return
	}
}

type arrOptTupleInfo struct {
	nArray   int     /* 'this' tuple's array's count */
	nTuple   int     /* 'this' tuple count in real */
	indices  []int   /* optional tuple's index in ts */
	versions []int64 /* optional tuple's version */
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

// AdjustOptionalTuples : according to tuple version, adjust some tuples' order in sub array tuple
func (n3ic *DBClient) AdjustOptionalTuples(ts *[]*pb.SPOTuple, arrInfo *map[string]int) {
	mapTuplesInArr := make(map[string]*arrOptTupleInfo)
	for ituple, tuple := range *ts {
		pre, ver := tuple.Predicate, tuple.Version
		if ok, nArr := u.Str(pre).CoverAnyKeyInMapSI(*arrInfo); ok {
			// fPf("%d : %s : %d # %d\n", ituple, pre, nArr, ver)
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
			// fPf("%d --- %v\n", tIdx, this)
			if vers, indicesTS := getVersByPre(ts, above.Predicate); len(vers) > 0 {
				tVer := v.versions[i]
				_, idx := u.I64(tVer).Nearest(vers...)
				posShouldBeAfter := indicesTS[idx]
				shouldBeAfter := (*ts)[posShouldBeAfter]
				// fPf("Should be after : %d --- %v\n", posShouldBeAfter, shouldBeAfter)
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
		tempts, _, _, _ = u.GArr(tempts).MoveItemAfter(
			func(move interface{}) bool { return move == optTuples[i] },
			func(after interface{}) bool { return after == aboveTuples[i] },
		)
	}
	for k, t := range tempts {
		(*ts)[k] = t.(*pb.SPOTuple)
	}
}

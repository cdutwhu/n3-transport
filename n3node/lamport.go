package n3node

import (
	"../n3influx"
	u "github.com/cdutwhu/go-util"
	"github.com/nsip/n3-messages/messages/pb"
)

func getValueVerRange(dbClient *n3influx.DBClient, objID string, ctx string) (start, end, ver int64) {
	tuple := &pb.SPOTuple{Subject: objID, Predicate: "V"}
	o, v := dbClient.GetObjVer(tuple, u.Str(ctx).MkSuffix("-meta"))
	if v != -1 {
		ss := sSpl(o, "-")
		start, end, ver = u.Str(ss[0]).ToInt64(), u.Str(ss[1]).ToInt64(), v
	}
	return
}

func assignVer(dbClient *n3influx.DBClient, tuple *pb.SPOTuple, ctx string) bool {
	s, p, o, v := tuple.Subject, tuple.Predicate, tuple.Object, tuple.Version

	// *** for value tuple ***
	if u.Str(s).IsUUID() {

		// *** New ID (NOT Terminator) is coming ***
		if s != prevID && p != TERMMARK {
			mapIDVQ[s] = append(mapIDVQ[s], v)
			l := len(mapIDVQ[s])
			fPln(l, mapIDVQ[s])
			startVer = mapIDVQ[s][l-1]
		}

		// *** Terminator is coming, save ***
		if p == TERMMARK {
			// *** Save prevID's low-high version map into meta db as <"id" - "" - "low-high"> ***
			dbClient.StoreTuple(
				&pb.SPOTuple{
					Subject:   prevID,
					Predicate: "V",
					Object:    fSf("%d-%d", startVer, prevVer),
					Version:   verMeta,
				},
				ctx+"-meta") // *** Meta Context ***
			verMeta++
		}

		prevID, prevPred, prevVer = s, p, v
	}

	// *** check struct tuple ***
	if p == "::" {
		if objDB, verDB := dbClient.GetObjVer(tuple, ctx); verDB > 0 {
			if u.Str(objDB).FieldsSeqContain(o, " + ") {
				tuple.Version = 0
				return false
			}
		}
	}

	return true
}

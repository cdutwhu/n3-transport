package n3node

import (
	"../n3influx"
	u "github.com/cdutwhu/go-util"
	"github.com/nsip/n3-messages/messages/pb"
)

func assignVer(dbClient *n3influx.DBClient, tuple *pb.SPOTuple, ctx string) {
	_, pred, obj, ver := tuple.Subject, tuple.Predicate, tuple.Object, tuple.Version

	// *** check struct tuples ***
	if pred == "::" {
		if objDB, verDB, found := dbClient.GetObj(tuple, 0, false, ctx); found && ver >= verDB {
			if u.Str(objDB).FieldsSeqContain(obj, " + ") {
				// fPln("Version assign 1...")
				tuple.Version = 0
				return
			}
		}
	}

	if objDB, _, found := dbClient.GetObj(tuple, 0, false, ctx); found && objDB == obj {
		tuple.Version = 0
		return
	}

	// // *** check array info tuples ***
	// if u.Str(pred).IsUUID() {
	// 	if objDB, verDB, found := dbClient.GetObj(tuple, 0, false, ctx); found && ver >= verDB {
	// 		if objDB == obj {
	// 			// fPln("Version assign 2...")
	// 			tuple.Version = 0
	// 		}
	// 	}
	// }
}

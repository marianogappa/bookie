package main

type accumulators struct {
	t map[string]map[string]float64
}

func (t *accumulators) add(fsmId, fsmAlias, k string, v float64) {
	if t.t == nil {
		t.t = map[string]map[string]float64{}
	}

	key := concatKeys(fsmId, fsmAlias)

	if _, ok := t.t[key]; !ok {
		t.t[key] = map[string]float64{k: v}
		return
	}
	t.t[key][k] += v
}

func (t *accumulators) flush() []query {
	var qs []query

	if len(t.t) == 0 {
		return qs
	}

	var vs, tmpVs []interface{}
	for key, aux := range t.t {
		for k, v := range aux {
			fsmID, fsmAlias := extractKeys(key)

			if fsmID == "" {
				tmpVs = append(tmpVs, fsmAlias, k, v)
			} else {
				vs = append(vs, fsmID, k, v)
			}
		}
	}

	t.t = nil

	if len(tmpVs) > 0 {
		qs = append(qs, query{
			sql: `INSERT INTO bookie.tmpAccumulators(fsmAlias, k, v) VALUES ` +
				buildInsertTuples(3, len(tmpVs)/3) +
				` ON DUPLICATE KEY UPDATE v = v + VALUES(v)`,
			values: tmpVs,
		})
	}

	if len(vs) > 0 {
		qs = append(qs, query{
			sql: `INSERT INTO bookie.accumulators(fsmID, k, v) VALUES ` +
				buildInsertTuples(3, len(vs)/3) +
				` ON DUPLICATE KEY UPDATE v = v + VALUES(v)`,
			values: vs,
		})
	}

	return qs
}

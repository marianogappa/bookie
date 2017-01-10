package main

type tags struct {
	t map[string]map[string]string
}

func (t *tags) add(fsmId, fsmAlias, k, v string) {

	if t.t == nil {
		t.t = map[string]map[string]string{}
	}

	key := concatKeys(fsmId, fsmAlias)

	if _, ok := t.t[key]; !ok {
		t.t[key] = map[string]string{k: v}
		return
	}
	t.t[key][k] = v
}

func (t *tags) flush() []query {
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
			sql: `INSERT INTO bookie.tmpTags(fsmAlias, k, v) VALUES ` +
				buildInsertTuples(3, len(tmpVs)/3) +
				` ON DUPLICATE KEY UPDATE v = VALUES(v)`,
			values: tmpVs,
		})
	}

	if len(vs) > 0 {
		qs = append(qs, query{
			sql: `INSERT INTO bookie.tags(fsmID, k, v) VALUES ` +
				buildInsertTuples(3, len(vs)/3) +
				` ON DUPLICATE KEY UPDATE v = VALUES(v)`,
			values: vs,
		})
	}

	return qs
}

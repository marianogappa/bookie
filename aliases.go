package main

type aliases struct {
	a map[string]string
}

func (a *aliases) add(fsmId string, fsmAlias string) {
	if a.a == nil {
		a.a = map[string]string{}
	}

	if _, ok := a.a[fsmId]; !ok {
		a.a[fsmId] = fsmAlias
	}
}

func (a *aliases) flush() *query {
	if len(a.a) == 0 {
		return nil
	}

	vs := make([]interface{}, len(a.a)*2)
	i := 0
	for fsmId, fsmAlias := range a.a {
		vs[i] = fsmId
		i++
		vs[i] = fsmAlias
		i++
	}

	a.a = nil

	return &query{
		`INSERT INTO bookie.fsmAliases(fsmID, fsmAlias) VALUES ` +
			buildInsertTuples(2, len(vs)/2) +
			` ON DUPLICATE KEY UPDATE fsmID=fsmID`,
		vs,
	}
}

package idgen

type OverCostActionArg struct {
	ActionType             int32
	TimeTick               int64
	WorkerId               uint16
	OverCostCountInOneTerm int32
	GenCountInOneTerm      int32
	TermIndex              int32
}

func (ocaa *OverCostActionArg) OverCostActionArg(workerId uint16, timeTick int64, actionType int32, overCostCountInOneTerm int32, genCountWhenOverCost int32, index int32) {
	ocaa.ActionType = actionType
	ocaa.TimeTick = timeTick
	ocaa.WorkerId = workerId
	ocaa.OverCostCountInOneTerm = overCostCountInOneTerm
	ocaa.GenCountInOneTerm = genCountWhenOverCost
	ocaa.TermIndex = index
}

package sazanami

// StageInfo describes a stage during execution.
type StageInfo struct {
	Index      int
	Name       string
	Tags       []string
	Attributes map[string]string
}

// ItemInfo describes an item processed within a stage.
type ItemInfo struct {
	Sequence uint64
	Worker   int
	Attempt  int
}

// Hooks captures lifecycle callbacks for observability.
type Hooks struct {
	StageStart    func(StageInfo)
	StageComplete func(StageInfo)
	StageError    func(StageInfo, error)

	ItemStart    func(StageInfo, ItemInfo)
	ItemComplete func(StageInfo, ItemInfo)
	ItemError    func(StageInfo, ItemInfo, error)
}

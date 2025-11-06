package sazanami

// FanOutBy routes each item from src to a named branch based on selector.
// The selector returns the branch name for the given item.
func FanOutBy[T any](src <-chan T, selector func(T) string, branches ...string) map[string]*Builder[T, T] {
	if selector == nil {
		panic("sazanami: selector required for FanOutBy")
	}

	uniq := make(map[string]struct{})
	for _, name := range branches {
		uniq[name] = struct{}{}
	}

	outCh := make(map[string]chan T)
	result := make(map[string]*Builder[T, T])

	for name := range uniq {
		ch := make(chan T)
		outCh[name] = ch
		result[name] = From(getSource(ch))
	}

	go func() {
		defer func() {
			for _, ch := range outCh {
				close(ch)
			}
		}()
		for v := range src {
			branch := selector(v)
			ch, ok := outCh[branch]
			if !ok {
				ch = make(chan T)
				outCh[branch] = ch
				result[branch] = From(getSource(ch))
			}
			ch <- v
		}
	}()

	return result
}

func getSource[T any](src <-chan T) <-chan T {
	return src
}

package engine

// StreamElement is anything that flows through the engine's channels: an Event
// or a Watermark. Watermarks travel in-band — on the same channel as events,
// never out-of-band — so ordering guarantees hold. The mental model (as in
// Flink) is "a watermark is just another thing in the stream."
//
// The interface is sealed (the marker method is unexported), so only Event and
// Watermark can be stream elements.
type StreamElement interface {
	streamElement()
}

func (Event) streamElement()     {}
func (Watermark) streamElement() {}

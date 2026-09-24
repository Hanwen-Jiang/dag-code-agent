package xai

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"image"
	_ "image/jpeg" // register decoders
	_ "image/png"  // register decoders
	"sort"
	"strings"
	"sync"
)

// Modality names.
const (
	ModalityText       = "text"
	ModalityImage      = "image"
	ModalityAudio      = "audio"
	ModalityScreenshot = "screenshot"
)

// What is real and what is simulated in this package:
//
//   - REAL: modality routing, capability negotiation, size/limit validation,
//     image decoding via the stdlib image package (JPEG/PNG), deterministic
//     content hashing, structured metadata extraction, audio WAV header
//     parsing, and the processor registry.
//   - SIMULATED (no real models are available offline): OCR text extraction
//     and natural-language image description. The reference OCR processor
//     returns a clearly-labeled structured placeholder; a real OCR engine can
//     be swapped in via RegisterProcessor.

// ProcessResult is the structured output of a modality processor.
type ProcessResult struct {
	Modality string `json:"modality"`
	OK       bool   `json:"ok"`
	// Fields is structured, deterministic output (dimensions, hashes, ...).
	Fields map[string]interface{} `json:"fields"`
	// Simulated flags which fields were produced by a reference (non-model)
	// implementation rather than a real engine.
	Simulated []string `json:"simulated,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// ProcessorCapability describes what a processor can do, used during
// capability negotiation.
type ProcessorCapability struct {
	Modality string
	Features []string // e.g. "ocr", "dimensions", "hash", "transcribe"
}

// Processor handles one modality. Implementations must be deterministic and
// safe for concurrent use.
type Processor interface {
	// Capabilities reports what this processor can do.
	Capabilities() ProcessorCapability
	// Process runs the processor over the input bytes.
	Process(data []byte) ProcessResult
}

// ProcessorRequest is one multimodal input.
type ProcessorRequest struct {
	Modality string
	Data     []byte
	// RequestedFeatures are the features the caller wants; used for
	// capability negotiation.
	RequestedFeatures []string
}

// NegotiationResult is the outcome of matching a request to a processor.
type NegotiationResult struct {
	Processor   string   // processor name chosen
	Modality    string   // modality it will handle
	Supported   []string // requested features the processor supports
	Unsupported []string // requested features it cannot provide
	OK          bool
	Reason      string
}

// MultimodalRouter routes multimodal inputs to processors with capability
// negotiation. It is a registry: register one or more processors per modality
// and the router picks one whose capabilities cover the request (most
// supported features first, then registration order).
type MultimodalRouter struct {
	mu         sync.RWMutex
	processors []namedProcessor
}

type namedProcessor struct {
	name string
	p    Processor
}

// NewMultimodalRouter creates an empty router.
func NewMultimodalRouter() *MultimodalRouter {
	return &MultimodalRouter{processors: make([]namedProcessor, 0)}
}

// Register adds a processor. Later registrations with the same name replace
// earlier ones.
func (r *MultimodalRouter) Register(name string, p Processor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, np := range r.processors {
		if np.name == name {
			r.processors[i] = namedProcessor{name: name, p: p}
			return
		}
	}
	r.processors = append(r.processors, namedProcessor{name: name, p: p})
}

// Negotiate matches a request to the best available processor without running
// it. Returns OK=false with a reason when no processor covers the modality.
func (r *MultimodalRouter) Negotiate(req ProcessorRequest) NegotiationResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var best *namedProcessor
	bestScore := -1
	for i := range r.processors {
		np := &r.processors[i]
		caps := np.p.Capabilities()
		if !strings.EqualFold(caps.Modality, req.Modality) {
			continue
		}
		score := 0
		for _, f := range req.RequestedFeatures {
			if hasFeature(caps.Features, f) {
				score++
			}
		}
		if score > bestScore {
			best, bestScore = np, score
		}
	}
	if best == nil {
		return NegotiationResult{OK: false, Reason: fmt.Sprintf("no processor registered for modality %q", req.Modality)}
	}

	res := NegotiationResult{Processor: best.name, Modality: best.p.Capabilities().Modality, OK: true}
	for _, f := range req.RequestedFeatures {
		if hasFeature(best.p.Capabilities().Features, f) {
			res.Supported = append(res.Supported, f)
		} else {
			res.Unsupported = append(res.Unsupported, f)
		}
	}
	return res
}

// Process negotiates then runs the chosen processor.
func (r *MultimodalRouter) Process(req ProcessorRequest) (*ProcessResult, error) {
	neg := r.Negotiate(req)
	if !neg.OK {
		return nil, fmt.Errorf("%s", neg.Reason)
	}
	r.mu.RLock()
	var p Processor
	for i := range r.processors {
		if r.processors[i].name == neg.Processor {
			p = r.processors[i].p
			break
		}
	}
	r.mu.RUnlock()
	if p == nil {
		return nil, fmt.Errorf("processor %q vanished during negotiation", neg.Processor)
	}
	res := p.Process(req.Data)
	return &res, nil
}

func hasFeature(features []string, f string) bool {
	for _, x := range features {
		if strings.EqualFold(x, f) {
			return true
		}
	}
	return false
}

// ---- reference processors (deterministic, stdlib only) ----

// ImageProcessor extracts real, deterministic metadata from JPEG/PNG bytes:
// dimensions, format, byte size, and a SHA-256 content hash. OCR and
// description are exposed as simulated features (structured placeholders) and
// are flagged in the result.
type ImageProcessor struct {
	// EnableOCR toggles the simulated OCR feature.
	EnableOCR bool
	// MaxBytes limits input size (0 = unlimited).
	MaxBytes int
}

// Capabilities implements Processor.
func (p *ImageProcessor) Capabilities() ProcessorCapability {
	features := []string{"dimensions", "format", "hash", "size"}
	if p.EnableOCR {
		features = append(features, "ocr")
	}
	features = append(features, "description")
	return ProcessorCapability{Modality: ModalityImage, Features: features}
}

// Process implements Processor.
func (p *ImageProcessor) Process(data []byte) ProcessResult {
	res := ProcessResult{
		Modality: ModalityImage,
		Fields:   make(map[string]interface{}),
	}
	if p.MaxBytes > 0 && len(data) > p.MaxBytes {
		res.OK = false
		res.Error = fmt.Sprintf("image exceeds %d byte limit (%d)", p.MaxBytes, len(data))
		return res
	}
	if len(data) == 0 {
		res.OK = false
		res.Error = "empty image data"
		return res
	}

	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		res.OK = false
		res.Error = fmt.Sprintf("decode image: %v", err)
		return res
	}
	bounds := img.Bounds()
	res.Fields["width"] = bounds.Dx()
	res.Fields["height"] = bounds.Dy()
	res.Fields["format"] = format
	res.Fields["size_bytes"] = len(data)
	sum := sha256.Sum256(data)
	res.Fields["sha256"] = fmt.Sprintf("%x", sum)

	res.OK = true

	if p.EnableOCR {
		// Simulated OCR: real text extraction requires an OCR engine, which is
		// not available offline. Return a clearly-labeled placeholder.
		res.Fields["ocr_text"] = ""
		res.Fields["ocr_available"] = false
		res.Simulated = append(res.Simulated, "ocr")
	}
	// Simulated description: a deterministic caption from real metadata only.
	res.Fields["description"] = fmt.Sprintf("%s image %dx%d (%s, %d bytes)",
		format, bounds.Dx(), bounds.Dy(), format, len(data))
	res.Simulated = append(res.Simulated, "description")
	return res
}

// AudioProcessor parses WAV headers (real) and offers a simulated transcribe
// feature. Non-WAV audio is rejected with a clear error.
type AudioProcessor struct{}

// Capabilities implements Processor.
func (p *AudioProcessor) Capabilities() ProcessorCapability {
	return ProcessorCapability{Modality: ModalityAudio, Features: []string{"wav_header", "duration", "hash", "transcribe"}}
}

// Process implements Processor.
func (p *AudioProcessor) Process(data []byte) ProcessResult {
	res := ProcessResult{
		Modality: ModalityAudio,
		Fields:   make(map[string]interface{}),
	}
	if len(data) == 0 {
		res.OK = false
		res.Error = "empty audio data"
		return res
	}
	sum := sha256.Sum256(data)
	res.Fields["sha256"] = fmt.Sprintf("%x", sum)
	res.Fields["size_bytes"] = len(data)

	if len(data) < 44 || !bytes.Equal(data[:4], []byte("RIFF")) || !bytes.Equal(data[8:12], []byte("WAVE")) {
		res.OK = false
		res.Error = "audio is not a WAV file (RIFF/WAVE header missing); only WAV is parsed offline"
		return res
	}

	// Parse WAV fmt chunk (little-endian).
	readU16 := func(off int) uint16 { return uint16(data[off]) | uint16(data[off+1])<<8 }
	readU32 := func(off int) uint32 {
		return uint32(data[off]) | uint32(data[off+1])<<8 | uint32(data[off+2])<<16 | uint32(data[off+3])<<24
	}
	audioFormat := readU16(20)
	channels := int(readU16(22))
	sampleRate := int(readU32(24))
	byteRate := int(readU32(28))
	bitsPerSample := int(readU16(34))

	res.Fields["format"] = "wav"
	res.Fields["audio_format_code"] = audioFormat
	res.Fields["channels"] = channels
	res.Fields["sample_rate"] = sampleRate
	res.Fields["bits_per_sample"] = bitsPerSample
	if byteRate > 0 && len(data) > 44 {
		dataBytes := len(data) - 44
		durationSec := float64(dataBytes) / float64(byteRate)
		res.Fields["duration_seconds"] = durationSec
	}
	res.OK = true

	// Simulated transcription.
	res.Fields["transcript"] = ""
	res.Fields["transcribe_available"] = false
	res.Simulated = append(res.Simulated, "transcribe")
	return res
}

// TextProcessor normalizes and fingerprints text (real, no simulation).
type TextProcessor struct{}

// Capabilities implements Processor.
func (p *TextProcessor) Capabilities() ProcessorCapability {
	return ProcessorCapability{Modality: ModalityText, Features: []string{"normalize", "tokens", "hash"}}
}

// Process implements Processor.
func (p *TextProcessor) Process(data []byte) ProcessResult {
	res := ProcessResult{
		Modality: ModalityText,
		Fields:   make(map[string]interface{}),
	}
	text := string(data)
	words := strings.Fields(text)
	sum := sha256.Sum256(data)
	res.Fields["char_count"] = len(text)
	res.Fields["word_count"] = len(words)
	res.Fields["sha256"] = fmt.Sprintf("%x", sum)
	res.OK = true
	return res
}

// DefaultMultimodalRouter builds a router pre-loaded with the reference
// processors: text, image (OCR enabled) and audio.
func DefaultMultimodalRouter() *MultimodalRouter {
	r := NewMultimodalRouter()
	r.Register("text-ref", &TextProcessor{})
	r.Register("image-ref", &ImageProcessor{EnableOCR: true})
	r.Register("audio-ref", &AudioProcessor{})
	return r
}

// SortedFeatures returns the supported features of a negotiation in stable
// order (helper for deterministic reporting).
func SortedFeatures(features []string) []string {
	out := make([]string, len(features))
	copy(out, features)
	sort.Strings(out)
	return out
}

// DecodeBase64 decodes a base64 payload, erroring on bad input.
func DecodeBase64(payload string) ([]byte, error) {
	if payload == "" {
		return nil, fmt.Errorf("empty base64 payload")
	}
	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		// Also try the URL alphabet and no-padding encodings as a convenience.
		if b2, err2 := base64.URLEncoding.DecodeString(payload); err2 == nil {
			return b2, nil
		}
		if b3, err3 := base64.RawStdEncoding.DecodeString(payload); err3 == nil {
			return b3, nil
		}
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	return b, nil
}

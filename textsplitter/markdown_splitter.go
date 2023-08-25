package textsplitter

import (
	"fmt"
	"reflect"

	"gitlab.com/golang-commonmark/markdown"
)

// NewMarkdownHeaderTextSplitter creates a new markdown header text splitter.
func NewMarkdownHeaderTextSplitter(options ...Option) *MarkdownHeaderTextSplitter {
	sp := &MarkdownHeaderTextSplitter{
		ChunkSize:      _defaultTokenChunkSize,
		ChunkOverlap:   _defaultTokenChunkOverlap,
		SecondSplitter: NewTokenSplitter(),
	}

	var opts Options
	for _, option := range options {
		option(&opts)
	}

	if opts.ChunkSize != 0 {
		sp.ChunkSize = opts.ChunkSize
	}

	if opts.ChunkOverlap != 0 {
		sp.ChunkOverlap = opts.ChunkOverlap
	}

	return sp
}

var _ TextSplitter = (*MarkdownHeaderTextSplitter)(nil)

// MarkdownHeaderTextSplitter markdown header text splitter
type MarkdownHeaderTextSplitter struct {
	ChunkSize    int
	ChunkOverlap int
	// SecondSplitter splits paragraphs
	SecondSplitter TextSplitter
}

// SplitText splits a text into multiple text.
func (sp MarkdownHeaderTextSplitter) SplitText(s string) ([]string, error) {
	mdParser := markdown.New(markdown.XHTMLOutput(true))
	tokens := mdParser.Parse([]byte(s))

	mc := &markdownContext{
		startAt:        0,
		endAt:          len(tokens),
		tokens:         tokens,
		chunkSize:      sp.ChunkSize,
		secondSplitter: sp.SecondSplitter,
	}

	chunks := mc.splitText()
	if len(chunks) == 0 {
		return sp.SecondSplitter.SplitText(s)
	}

	return chunks, nil
}

// markdownContext the helper
type markdownContext struct {
	// startAt represents the start position of the cursor in tokens
	startAt int
	// endAt represents the end position of the cursor in tokens
	endAt int
	// tokens represents the markdown tokens
	tokens []markdown.Token

	// hContent represents the current header(H1、H2 etc.) content
	hContent string
	// hContentAppend represents whether hContent has been appended to chunks
	hContentAppend bool
	// indentLevel represents the current indent level for ordered and unordered lists
	indentLevel int

	// chunks represents the final chunks
	chunks []string
	// curSnippet represents the current short markdown-format chunk
	curSnippet string
	// chunkSize represents the max chunk size, when exceeds, it will be split again
	chunkSize int
	// secondSplitter re-split markdown single long paragraph into chunks
	secondSplitter TextSplitter
}

func (mc *markdownContext) clone(startAt, endAt int) *markdownContext {
	subTokens := mc.tokens[startAt : endAt+1]
	return &markdownContext{
		endAt:          len(subTokens),
		tokens:         subTokens,
		hContent:       mc.hContent,
		hContentAppend: mc.hContentAppend,
		indentLevel:    mc.indentLevel,

		chunkSize:      mc.chunkSize,
		secondSplitter: mc.secondSplitter,
	}
}

func (mc *markdownContext) splitText() []string {
	for idx := mc.startAt; idx < mc.endAt; {
		token := mc.tokens[idx]
		switch token.(type) {
		case *markdown.HeadingOpen:
			mc.applyToChunks() // change header, apply to chunks
			mc.splitHeader()
		case *markdown.BulletListOpen:
			mc.splitBulletList()
		case *markdown.OrderedListOpen:
			mc.splitOrderedList()
		case *markdown.TableOpen:
			mc.splitTable()
		case *markdown.ParagraphOpen:
			mc.splitParagraph()
		case *markdown.BlockquoteOpen:
			mc.splitQuote()
		default:
			mc.startAt = indexOfCloseTag(mc.tokens, idx) + 1
		}
		idx = mc.startAt
	}

	// apply the last chunk
	mc.applyToChunks()

	return mc.chunks
}

// splitHeader splits H1/H2/.../H6
//
// format: HeadingOpen/Inline/HeadingClose
func (mc *markdownContext) splitHeader() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	header, ok := mc.tokens[mc.startAt].(*markdown.HeadingOpen)
	if !ok {
		return
	}

	// check next token is Inline
	inline, ok := mc.tokens[mc.startAt+1].(*markdown.Inline)
	if !ok {
		return
	}

	hm := repeatString(header.HLevel, "#")
	mc.hContent = fmt.Sprintf("%s %s", hm, inline.Content)

	return
}

// splitParagraph splits paragraph
//
// format: ParagraphOpen/Inline/ParagraphClose
func (mc *markdownContext) splitParagraph() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	inline, ok := mc.tokens[mc.startAt+1].(*markdown.Inline)
	if !ok {
		return
	}

	mc.splitInline(inline)
}

// splitQuote splits blockquote
//
// format: BlockquoteOpen/[Any]*/BlockquoteClose
func (mc *markdownContext) splitQuote() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	tmpMC := mc.clone(mc.startAt+1, endAt-1)
	chunks := tmpMC.splitText()

	mc.chunks = append(mc.chunks, chunks...)
}

// splitBulletList splits bullet list
//
// format: BulletListOpen/[ListItem]*/BulletListClose
func (mc *markdownContext) splitBulletList() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	mc.indentLevel++
	oldHContent := mc.hContent

	// move to ListItemOpen
	mc.startAt += 1

	// TODO: use `1.` as mark
	mc.splitListItem("*", endAt)

	// reset header mark
	mc.hContent = oldHContent
	mc.indentLevel--
}

// splitOrderedList splits ordered list
//
// format: BulletListOpen/[ListItem]*/BulletListClose
func (mc *markdownContext) splitOrderedList() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	mc.indentLevel++
	oldHContent := mc.hContent

	// move to ListItemOpen
	mc.startAt += 1

	mc.splitListItem("-", endAt)

	// reset header mark
	mc.hContent = oldHContent
	mc.indentLevel--
}

// splitInline splits inline
//
// format: Link/Image/Text
func (mc *markdownContext) splitInline(inline *markdown.Inline) {
	if link, ok := inline.Children[0].(*markdown.LinkOpen); ok && len(inline.Children) == 3 {
		mc.joinSnippet(fmt.Sprintf(`[%s](%s)`, link.Title, link.Href))
		return
	}

	if _, ok := inline.Children[0].(*markdown.Image); ok {
		return
	}

	mc.joinSnippet(inline.Content)
}

// splitListItem splits list item for bullet list and ordered list
//
// format: ListItemOpen/[ParagraphOpen/Inline/ParagraphClose/Any]*/ListItemClose
func (mc *markdownContext) splitListItem(mark string, endAt int) {
	item := mc.tokens[mc.startAt]
	if _, ok := item.(*markdown.ListItemOpen); !ok {
		return
	}

	// check inline before paragraph
	inline, ok := mc.tokens[mc.startAt+2].(*markdown.Inline)
	if !ok {
		return
	}

	listTitle := fmt.Sprintf("%s%s %s", repeatString(mc.indentLevel-1, "\t"), mark, inline.Content)
	// move to the next token after ParagraphClose
	mc.startAt += 4

	// check there is any other tokens belongs to current BulletList or OrderedList
	if mc.startAt < endAt {
		// check next token is ListItemOpen
		_, ok := mc.tokens[mc.startAt+1].(*markdown.ListItemOpen)
		if ok {
			// append current list title to current chunk
			mc.joinSnippet(listTitle)

			// move to the next ListItemOpen
			mc.startAt += 1

			// recursive to get all the list items
			mc.splitListItem(mark, endAt)
			return
		}

		// check next token is ParagraphOpen or any other tokens
		tempMC := mc.clone(mc.startAt, endAt-1)
		tempMC.hContent = listTitle
		tempChunks := tempMC.splitText()

		// append sub chunks to current chunk
		for _, chunk := range tempChunks {
			mc.joinSnippet(chunk)
		}
	}
}

// splitTable splits table
//
// format: TableOpen/THeadOpen/[*]/THeadClose/TBodyOpen/[*]/TBodyClose/TableClose
func (mc *markdownContext) splitTable() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	// check THeadOpen
	_, ok := mc.tokens[mc.startAt+1].(*markdown.TheadOpen)
	if !ok {
		return
	}

	// move to THeadOpen
	mc.startAt++

	// get table headers
	header := mc.splitTableHeader()
	// already move to TBodyOpen
	bodies := mc.splitTableBody()

	headnoteEmpty := false
	for _, h := range header {
		if h != "" {
			headnoteEmpty = true
			break
		}
	}

	// Sometime, there is no header in table, put the real table header to the first row
	if !headnoteEmpty && len(bodies) != 0 {
		header = bodies[0]
		bodies = bodies[1:]
	}

	// append table header
	for _, row := range bodies {
		for i, col := range row {
			mc.joinSnippet(fmt.Sprintf("%s\n%s", header[i], col))
		}
	}
}

// splitTableHeader splits table header
//
// format: THeadOpen/TrOpen/[ThOpen/Inline/ThClose]*/TrClose/THeadClose
func (mc *markdownContext) splitTableHeader() []string {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	// check TrOpen
	if _, ok := mc.tokens[mc.startAt+1].(*markdown.TrOpen); ok {
		return []string{}
	}

	var headers []string

	// move to TrOpen
	mc.startAt++

	for {
		// check ThOpen
		if _, ok := mc.tokens[mc.startAt+1].(*markdown.ThOpen); !ok {
			break
		}
		// move to ThOpen
		mc.startAt++

		// move to Inline
		mc.startAt++
		inline, ok := mc.tokens[mc.startAt].(*markdown.Inline)
		if !ok {
			break
		}

		headers = append(headers, inline.Content)
	}

	return headers
}

// splitTableBody splits table body
//
// format: TBodyOpen/TrOpen/[TdOpen/Inline/TdClose]*/TrClose/TBodyClose
func (mc *markdownContext) splitTableBody() [][]string {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	var rows [][]string

	for {
		// check TrOpen
		if _, ok := mc.tokens[mc.startAt+1].(*markdown.TrOpen); !ok {
			return rows
		}

		var row []string
		// move to TrOpen
		mc.startAt++
		colIdx := 0
		for {
			// check TdOpen
			if _, ok := mc.tokens[mc.startAt+1].(*markdown.TdOpen); !ok {
				break
			}

			// move to TdOpen
			mc.startAt++

			// move to Inline
			mc.startAt++
			inline, ok := mc.tokens[mc.startAt].(*markdown.Inline)
			if !ok {
				break
			}

			row = append(row, inline.Content)

			// move to TdClose
			mc.startAt++
			colIdx++
		}

		rows = append(rows, row)
		// move to TrClose
		mc.startAt++
	}
}

// joinSnippet join sub snippet to current total snippet
func (mc *markdownContext) joinSnippet(snippet string) {
	if mc.curSnippet == "" {
		mc.curSnippet = snippet
		return
	}

	// append snippet to current chunk with new line
	mc.curSnippet = fmt.Sprintf("%s\n\n%s", mc.curSnippet, snippet)

	// check whether current chunk exceeds chunk size, if so, apply to chunks
	if len(mc.curSnippet) > mc.chunkSize {
		mc.applyToChunks()
	}

	return
}

// applyToChunks 将当前分块内容添加到 chunks 中
func (mc *markdownContext) applyToChunks() {
	defer func() {
		mc.curSnippet = ""
		mc.hContentAppend = false
	}()

	// check whether current chunk is over ChunkSize，if so, re-split current chunk
	chunks, err := mc.secondSplitter.SplitText(mc.curSnippet)
	if err != nil {
		return
	}

	// if there is only H1/H2 and so on, just apply the `Header Title` to chunks
	if len(chunks) == 0 && mc.hContent != "" && !mc.hContentAppend {
		mc.chunks = append(mc.chunks, mc.hContent)
		mc.hContentAppend = true
		return
	}

	for _, chunk := range chunks {
		if chunk == "" {
			continue
		}

		mc.hContentAppend = true

		// prepend `Header Title` to chunk
		chunk := fmt.Sprintf("%s\n%s", mc.hContent, chunk)
		mc.chunks = append(mc.chunks, chunk)
	}
}

// repeatString repeats the initChar for count times
func repeatString(count int, initChar string) string {
	var s string
	for i := 0; i < count; i++ {
		s += initChar
	}
	return s
}

// closeTypes represents the close operation type for each open operation type
var closeTypes = map[reflect.Type]reflect.Type{
	reflect.TypeOf(&markdown.HeadingOpen{}):     reflect.TypeOf(&markdown.HeadingClose{}),
	reflect.TypeOf(&markdown.BulletListOpen{}):  reflect.TypeOf(&markdown.BulletListClose{}),
	reflect.TypeOf(&markdown.OrderedListOpen{}): reflect.TypeOf(&markdown.OrderedListClose{}),
	reflect.TypeOf(&markdown.ParagraphOpen{}):   reflect.TypeOf(&markdown.ParagraphClose{}),
	reflect.TypeOf(&markdown.BlockquoteOpen{}):  reflect.TypeOf(&markdown.BlockquoteClose{}),
	reflect.TypeOf(&markdown.ListItemOpen{}):    reflect.TypeOf(&markdown.ListItemClose{}),
	reflect.TypeOf(&markdown.Fence{}):           reflect.TypeOf(&markdown.Fence{}),
	reflect.TypeOf(&markdown.TableOpen{}):       reflect.TypeOf(&markdown.TableClose{}),
	reflect.TypeOf(&markdown.TheadOpen{}):       reflect.TypeOf(&markdown.TheadClose{}),
	reflect.TypeOf(&markdown.TbodyOpen{}):       reflect.TypeOf(&markdown.TbodyClose{}),
}

// indexOfCloseTag returns the index of the close tag for the open tag at startAt
func indexOfCloseTag(tokens []markdown.Token, startAt int) (end int) {
	sameCount := 0
	openType := reflect.ValueOf(tokens[startAt]).Type()
	closeType := closeTypes[openType]

	idx := startAt + 1
	for ; idx < len(tokens); idx++ {
		cur := reflect.ValueOf(tokens[idx]).Type()

		if openType == cur {
			sameCount++
		}

		if closeType == cur {
			if sameCount == 0 {
				break
			}
			sameCount--
		}
	}

	return idx
}

package textsplitter

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"gitlab.com/golang-commonmark/markdown"
)

// NewMarkdownHeaderTextSplitter creates a new markdown header text splitter.
func NewMarkdownHeaderTextSplitter(opts ...Option) *MarkdownHeaderTextSplitter {
	options := DefaultOptions()

	for _, o := range opts {
		o(&options)
	}

	sp := &MarkdownHeaderTextSplitter{
		ChunkSize:      options.ChunkSize,
		ChunkOverlap:   options.ChunkOverlap,
		SecondSplitter: options.SecondSplitter,
	}

	if sp.SecondSplitter == nil {
		sp.SecondSplitter = NewRecursiveCharacter(
			WithChunkSize(options.ChunkSize),
			WithChunkOverlap(options.ChunkOverlap),
			WithSeparators([]string{
				"\n\n", // new line
				"\n",   // new line
				" ",    // space
			}),
		)
	}

	return sp
}

var _ TextSplitter = (*MarkdownHeaderTextSplitter)(nil)

// MarkdownHeaderTextSplitter markdown header text splitter.
//
// Now, we support H1/H2/H3/H4/H5/H6, BulletList, OrderedList, Table, Paragraph, Blockquote,
// other format will be ignored. If your origin document is HTML, you purify and convert to markdown,
// then split it.
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
		chunkOverlap:   sp.ChunkOverlap,
		secondSplitter: sp.SecondSplitter,
	}

	chunks := mc.splitText()

	return chunks, nil
}

// markdownContext the helper.
type markdownContext struct {
	// startAt represents the start position of the cursor in tokens
	startAt int
	// endAt represents the end position of the cursor in tokens
	endAt int
	// tokens represents the markdown tokens
	tokens []markdown.Token

	// hTitle represents the current header(H1、H2 etc.) content
	hTitle string
	// hTitlePrepended represents whether hTitle has been appended to chunks
	hTitlePrepended bool

	orderedList bool
	bulletList  bool
	listOrder   int

	// indentLevel represents the current indent level for ordered、unordered lists
	indentLevel int

	// chunks represents the final chunks
	chunks []string
	// curSnippet represents the current short markdown-format chunk
	curSnippet string
	// chunkSize represents the max chunk size, when exceeds, it will be split again
	chunkSize    int
	chunkOverlap int

	// secondSplitter re-split markdown single long paragraph into chunks
	secondSplitter TextSplitter
}

func (mc *markdownContext) clone(startAt, endAt int) *markdownContext {
	subTokens := mc.tokens[startAt : endAt+1]
	return &markdownContext{
		endAt:  len(subTokens),
		tokens: subTokens,
		// hTitle:          mc.hTitle,
		hTitlePrepended: mc.hTitlePrepended,
		indentLevel:     mc.indentLevel,

		orderedList: mc.orderedList,

		chunkSize:      mc.chunkSize,
		chunkOverlap:   mc.chunkOverlap,
		secondSplitter: mc.secondSplitter,
	}
}

func (mc *markdownContext) splitText() []string {
	for idx := mc.startAt; idx < mc.endAt; {
		token := mc.tokens[idx]
		switch token.(type) {
		case *markdown.HeadingOpen:
			mc.onMDHeader()
		case *markdown.TableOpen:
			mc.onMDTable()
		case *markdown.ParagraphOpen:
			mc.onMDParagraph()
		case *markdown.BlockquoteOpen:
			mc.onMDQuote()
		case *markdown.BulletListOpen:
			mc.onMDBulletList()
		case *markdown.OrderedListOpen:
			mc.onMDOrderedList()
		case *markdown.ListItemOpen:
			mc.onListItem()
		default:
			mc.startAt = indexOfCloseTag(mc.tokens, idx) + 1
		}
		idx = mc.startAt
	}

	// apply the last chunk
	mc.applyToChunks()

	return mc.chunks
}

// onMDHeader splits H1/H2/.../H6
//
// format: HeadingOpen/Inline/HeadingClose
func (mc *markdownContext) onMDHeader() {
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

	mc.applyToChunks() // change header, apply to chunks

	hm := repeatString(header.HLevel, "#")
	mc.hTitle = fmt.Sprintf("%s %s", hm, inline.Content)
	mc.hTitlePrepended = false
}

// onMDParagraph splits paragraph
//
// format: ParagraphOpen/Inline/ParagraphClose
func (mc *markdownContext) onMDParagraph() {
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

// onMDQuote splits blockquote
//
// format: BlockquoteOpen/[Any]*/BlockquoteClose
func (mc *markdownContext) onMDQuote() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	_, ok := mc.tokens[mc.startAt].(*markdown.BlockquoteOpen)
	if !ok {
		return
	}

	tmpMC := mc.clone(mc.startAt+1, endAt-1)
	tmpMC.hTitle = ""
	chunks := tmpMC.splitText()

	for _, chunk := range chunks {
		lines := strings.Split(chunk, "\n")
		for i, line := range lines {
			lines[i] = fmt.Sprintf("> %s", line)
		}
		chunk = strings.Join(lines, "\n")
		mc.joinSnippet(chunk)
	}

	mc.applyToChunks()
}

// onMDBulletList splits bullet list
//
// format: BulletListOpen/[ListItem]*/BulletListClose
func (mc *markdownContext) onMDBulletList() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
		mc.indentLevel--

		mc.bulletList = false
		mc.listOrder = 0
	}()

	mc.indentLevel++
	mc.bulletList = true

	// try move to ListItemOpen
	mc.startAt++

	tempMD := mc.clone(mc.startAt, endAt-1)
	tempChunk := tempMD.splitText()

	for _, chunk := range tempChunk {
		if tempMD.indentLevel > 1 {
			lines := strings.Split(chunk, "\n")
			for i, line := range lines {
				lines[i] = fmt.Sprintf("  %s", line)
			}
			chunk = strings.Join(lines, "\n")
		}

		mc.joinSnippet(chunk)
	}
}

// onMDOrderedList splits ordered list
//
// format: BulletListOpen/[ListItem]*/BulletListClose
func (mc *markdownContext) onMDOrderedList() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
		mc.indentLevel--

		mc.orderedList = false
		mc.listOrder = 0
	}()

	mc.indentLevel++

	// try move to ListItemOpen
	mc.startAt++
	mc.orderedList = true

	tempMD := mc.clone(mc.startAt, endAt-1)
	tempChunk := tempMD.splitText()

	for _, chunk := range tempChunk {
		if tempMD.indentLevel > 1 {
			lines := strings.Split(chunk, "\n")
			for i, line := range lines {
				lines[i] = fmt.Sprintf("  %s", line)
			}
			chunk = strings.Join(lines, "\n")
		}
		mc.joinSnippet(chunk)
	}
}

// onListItem the item of ordered list or unordered list, maybe contains sub BulletList or OrderedList.
// /
// format1: ListItemOpen/ParagraphOpen/Inline/ParagraphClose/ListItemClose
// format2: ListItemOpen/ParagraphOpen/Inline/ParagraphClose/[BulletList]*/ListItemClose
func (mc *markdownContext) onListItem() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	mc.startAt++

	for mc.startAt < endAt {
		nextToken := mc.tokens[mc.startAt]
		switch nextToken.(type) {
		case *markdown.ParagraphOpen:
			mc.onMDListItemParagraph()
		case *markdown.BulletListOpen:
			mc.onMDBulletList()
		case *markdown.OrderedListOpen:
			mc.onMDOrderedList()
		default:
			mc.startAt++
		}
	}
}

// onMDListItemParagraph splits list item paragraph
func (mc *markdownContext) onMDListItemParagraph() {
	endAt := indexOfCloseTag(mc.tokens, mc.startAt)
	defer func() {
		mc.startAt = endAt + 1
	}()

	inline, ok := mc.tokens[mc.startAt+1].(*markdown.Inline)
	if !ok {
		return
	}

	line := mc.splitInline(inline)
	if mc.orderedList {
		mc.listOrder++
		line = fmt.Sprintf("%d. %s", mc.listOrder, line)
	}

	if mc.bulletList {
		line = fmt.Sprintf("- %s", line)
	}

	mc.joinSnippet(line)
}

// onMDTable splits table
//
// format: TableOpen/THeadOpen/[*]/THeadClose/TBodyOpen/[*]/TBodyClose/TableClose
func (mc *markdownContext) onMDTable() {
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

	mc.splitTableRows(header, bodies)
}

// splitTableRows splits table rows, each row is a single Document.
//
//nolint:cyclop
func (mc *markdownContext) splitTableRows(header []string, bodies [][]string) {
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

	headerMD := ""
	for i, h := range header {
		headerMD += fmt.Sprintf("| %s ", h)
		if i == len(header)-1 {
			headerMD += "|"
		}
	}
	headerMD += "\n" // add new line

	for i := 0; i < len(header); i++ {
		headerMD += "| --- "
		if i == len(header)-1 {
			headerMD += "|"
		}
	}

	if len(bodies) == 0 {
		mc.joinSnippet(headerMD)
		mc.applyToChunks()
		return
	}

	// append table header
	for _, row := range bodies {
		line := ""
		for i := range row {
			line += fmt.Sprintf("| %s ", row[i])
			if i == len(row)-1 {
				line += "|"
			}
		}

		mc.joinSnippet(fmt.Sprintf("%s\n%s", headerMD, line))

		// keep every row in a single Document
		mc.applyToChunks()
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
	if _, ok := mc.tokens[mc.startAt+1].(*markdown.TrOpen); !ok {
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

		// move th ThClose
		mc.startAt++
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

// joinSnippet join sub snippet to current total snippet.
func (mc *markdownContext) joinSnippet(snippet string) {
	if mc.curSnippet == "" {
		mc.curSnippet = snippet
		return
	}

	// check whether current chunk exceeds chunk size, if so, apply to chunks
	if utf8.RuneCountInString(mc.curSnippet)+utf8.RuneCountInString(snippet) >= mc.chunkSize {
		mc.applyToChunks()
		mc.curSnippet = snippet
	} else {
		mc.curSnippet = fmt.Sprintf("%s\n%s", mc.curSnippet, snippet)
	}
}

// applyToChunks applies current snippet to chunks.
func (mc *markdownContext) applyToChunks() {
	defer func() {
		mc.curSnippet = ""
	}()

	var chunks []string
	// check whether current chunk is over ChunkSize，if so, re-split current chunk
	if utf8.RuneCountInString(mc.curSnippet) <= mc.chunkSize+mc.chunkOverlap {
		chunks = []string{mc.curSnippet}
	} else {
		// split current snippet to chunks
		chunks, _ = mc.secondSplitter.SplitText(mc.curSnippet)
	}

	// if there is only H1/H2 and so on, just apply the `Header Title` to chunks
	if len(chunks) == 0 && mc.hTitle != "" && !mc.hTitlePrepended {
		mc.chunks = append(mc.chunks, mc.hTitle)
		mc.hTitlePrepended = true
		return
	}

	for _, chunk := range chunks {
		if chunk == "" {
			continue
		}

		mc.hTitlePrepended = true
		if mc.hTitle != "" {
			// prepend `Header Title` to chunk
			chunk = fmt.Sprintf("%s\n%s", mc.hTitle, chunk)
		}
		mc.chunks = append(mc.chunks, chunk)
	}
}

// splitInline splits inline
//
// format: Link/Image/Text
func (mc *markdownContext) splitInline(inline *markdown.Inline) string {
	return inline.Content
}

// repeatString repeats the initChar for count times.
func repeatString(count int, initChar string) string {
	var s string
	for i := 0; i < count; i++ {
		s += initChar
	}
	return s
}

// closeTypes represents the close operation type for each open operation type.
var closeTypes = map[reflect.Type]reflect.Type{ //nolint:gochecknoglobals
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

// indexOfCloseTag returns the index of the close tag for the open tag at startAt.
func indexOfCloseTag(tokens []markdown.Token, startAt int) int {
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

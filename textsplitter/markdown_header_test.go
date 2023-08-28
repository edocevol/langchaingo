package textsplitter

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tmc/langchaingo/schema"
)

func TestMarkdownHeaderTextSplitter_SplitText(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name         string
		markdown     string
		expectedDocs []schema.Document
	}

	testCases := []testCase{
		{
			name: "split by header",
			markdown: `
### This is a header

- This is a list item of bullet type.
- This is another list item.

 *Everything* is going according to **plan**.
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `### This is a header
- This is a list item of bullet type.`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `### This is a header
- This is another list item.`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `### This is a header
*Everything* is going according to **plan**.`,
					Metadata: map[string]any{},
				},
			},
		},
		{
			name:     "hence will be ignored",
			markdown: "example code:\n```go\nfunc main() {}\n```",
			expectedDocs: []schema.Document{
				{PageContent: "example code:", Metadata: map[string]any{}},
			},
		},
	}

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDocs, docs)
		})
	}
}

// TestMarkdownHeaderTextSplitter_Table markdown always split by line
func TestMarkdownHeaderTextSplitter_Table(t *testing.T) {
	t.Parallel()
	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}
	testCases := []testCase{
		{
			markdown: `| Syntax      | Description |
| ----------- | ----------- |
| Header      | Title       |
| Paragraph   | Text        |`,
			expectedDocs: []schema.Document{
				{
					PageContent: `| Syntax | Description |
| --- | --- |
| Header | Title |`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `| Syntax | Description |
| --- | --- |
| Paragraph | Text |`,
					Metadata: map[string]any{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("chunk size 64", func(t *testing.T) {
			splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
			docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDocs, docs)
		})

		t.Run("chunk size 512", func(t *testing.T) {
			splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
			docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDocs, docs)
		})
	}
}

func TestMarkdownHeaderTextSplitter(t *testing.T) {
	data, err := os.ReadFile("../CONTRIBUTING.md")
	if err != nil {
		t.Fatal(err)
	}

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
	docs, err := CreateDocuments(splitter, []string{string(data)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, doc := range docs {
		t.Log(doc.PageContent)
	}
}

package textsplitter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tmc/langchaingo/schema"
)

func TestMarkdownHeaderTextSplitter_SplitText(t *testing.T) {
	t.Parallel()
	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}

	testCases := []testCase{
		{
			markdown: `
> ### This is a header
>
> - This is a list item of bullet type.
> - This is another list item.
>
>  *Everything* is going according to **plan**.
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `### This is a header
* This is a list item of bullet type.

* This is another list item.`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `### This is a header
*Everything* is going according to **plan**.`,
					Metadata: map[string]any{},
				}, //nolint:lll
			},
		},
		{
			markdown: `
| Syntax      | Description |
| ----------- | ----------- |
| Header      | Title       |
| Paragraph   | Text        |
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `Header 
Paragraph
nTitle
Text
`,
					Metadata: map[string]any{},
				},
			},
		},
	}

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(32))
	for _, tc := range testCases {
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

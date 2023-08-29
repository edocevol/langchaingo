package textsplitter

import (
	"log"
	"os"
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
			markdown: "example code:\n```go\nfunc main() {}\n```",
			expectedDocs: []schema.Document{
				{PageContent: "example code:", Metadata: map[string]any{}},
			},
		},
	}

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
	for _, tc := range testCases {
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

// TestMarkdownHeaderTextSplitter_Table markdown always split by line.
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
		splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)

		splitter = NewMarkdownHeaderTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
		docs, err = CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

func TestMarkdownHeaderTextSplitter(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("./testdata/example.md")
	if err != nil {
		t.Fatal(err)
	}

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
	docs, err := CreateDocuments(splitter, []string{string(data)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var pages string
	for _, doc := range docs {
		pages += doc.PageContent + "\n\n---\n\n"
	}

	err = os.WriteFile("./testdata/example_markdown_header_512.md", []byte(pages), os.ModeExclusive|os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMarkdownHeaderTextSplitter_BulletList(t *testing.T) {
	markdown := `
- [Code of Conduct](#code-of-conduct)
- [I Have a Question](#i-have-a-question)
- [I Want To Contribute](#i-want-to-contribute)
    - [Reporting Bugs](#reporting-bugs)
        - [Before Submitting a Bug Report](#before-submitting-a-bug-report)
        - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)
    - [Suggesting Enhancements](#suggesting-enhancements)
        - [Before Submitting an Enhancement](#before-submitting-an-enhancement)
        - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
    - [Your First Code Contribution](#your-first-code-contribution)
        - [Make Changes](#make-changes)
            - [Make changes in the UI](#make-changes-in-the-ui)
            - [Make changes locally](#make-changes-locally)
        - [Commit your update](#commit-your-update)
        - [Pull Request](#pull-request)
        - [Your PR is merged!](#your-pr-is-merged)
`

	splitter := NewMarkdownHeaderTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
	docs, err := CreateDocuments(splitter, []string{markdown}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, doc := range docs {
		log.Printf("%s\n-------------------\n", doc.PageContent)
	}
}

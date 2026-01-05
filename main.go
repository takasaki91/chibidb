package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	IdSize       = 4
	UsernameSize = 32
	EmailSize    = 255

	RowSize = IdSize + UsernameSize + EmailSize

	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

type Row struct {
	ID       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}

type Pager struct {
	File       *os.File
	FileLength int64
	Pages      [TableMaxPages][]byte
}

type Table struct {
	NumRows uint32
	Pager   *Pager
}

type Cursor struct {
	Table      *Table
	RowNum     uint32
	EndOfTable bool
}

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognizedCommand
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareSyntaxError
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

// -------------- Pager Implements ------------------
func NewPager(filename string) *Pager {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Error opening DB file", err)
		os.Exit(1)
	}

	stat, _ := file.Stat()
	fileLength := stat.Size()

	return &Pager{
		File:       file,
		FileLength: fileLength,
	}
}
func (p *Pager) GetPage(pageNum int) []byte {
	if pageNum > TableMaxPages {
		fmt.Println("Page number out of bounds.")
		os.Exit(1)
	}

	if p.Pages[pageNum] == nil {
		page := make([]byte, PageSize)
		numPages := int(p.FileLength / PageSize)

		if p.FileLength%PageSize != 0 {
			numPages += 1
		}

		if pageNum < numPages {
			_, err := p.File.ReadAt(page, int64(pageNum*PageSize))
			if err != nil && err != io.EOF {
				fmt.Println("Error reading file:", err)
				os.Exit(1)
			}

		}
		p.Pages[pageNum] = page
	}
	return p.Pages[pageNum]
}

func (p *Pager) Flush(pageNum int, size int) {
	if p.Pages[pageNum] == nil {
		return
	}

	offset := int64(pageNum * PageSize)
	_, err := p.File.WriteAt(p.Pages[pageNum][:size], offset)
	if err != nil {
		fmt.Println("Error writing to file.", err)
		os.Exit(1)
	}
}

// ----------- DB Connection ----------
func DbOpen(filename string) *Table {
	pager := NewPager(filename)
	numRows := uint32(pager.FileLength / RowSize)

	return &Table{
		Pager:   pager,
		NumRows: numRows,
	}
}

func (t *Table) DbClose() {
	pager := t.Pager
	numFullPages := int(t.NumRows / RowsPerPage)

	for i := 0; i < numFullPages; i++ {
		if pager.Pages[i] != nil {
			pager.Flush(i, PageSize)
			pager.Pages[i] = nil
		}
	}

	numAdditionalRows := t.NumRows % RowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.Pages[pageNum] != nil {
			size := int(numAdditionalRows * RowSize)
			pager.Flush(pageNum, size)
			pager.Pages[pageNum] = nil
		}
	}
	pager.File.Close()
}

// ---------- Row Logic --------

func (r *Row) Serialize(dst []byte) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, r.ID)
	buf.Write(r.Username[:])
	buf.Write(r.Email[:])

	copy(dst, buf.Bytes())
}
func Deserialize(src []byte) *Row {
	row := &Row{}
	reader := bytes.NewReader(src)

	binary.Read(reader, binary.LittleEndian, &row.ID)
	reader.Read(row.Username[:])
	reader.Read(row.Email[:])

	return row
}

func (t *Table) TableStart() *Cursor {
	cursor := &Cursor{
		Table:      t,
		RowNum:     0,
		EndOfTable: (t.NumRows == 0),
	}
	return cursor
}

func (t *Table) TableEnd() *Cursor {
	cursor := &Cursor{
		Table:      t,
		RowNum:     t.NumRows,
		EndOfTable: true,
	}
	return cursor
}

func (c *Cursor) Value() ([]byte, error) {
	rowNum := c.RowNum
	pageNum := int(rowNum / RowsPerPage)

	page := c.Table.Pager.GetPage(pageNum)

	rowOffset := (rowNum % RowsPerPage) * RowSize
	return page[rowOffset : rowOffset+RowSize], nil
}

func (c *Cursor) Advance() {
	c.RowNum += 1
	if c.RowNum >= c.Table.NumRows {
		c.EndOfTable = true
	}
}

// --------- front ---------

func doMetaCommand(input string, table *Table) MetaCommandResult {
	if input == ".exit" {
		table.DbClose()
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if strings.HasPrefix(input, "insert") {
		statement.Type = StatementInsert
		var id uint32
		var username, email string

		n, err := fmt.Sscanf(input, "insert %d %s %s", &id, &username, &email)
		if err != nil || n < 3 {
			return PrepareSyntaxError
		}

		statement.RowToInsert.ID = id
		copy(statement.RowToInsert.Username[:], []byte(username))
		copy(statement.RowToInsert.Email[:], []byte(email))
		return PrepareSuccess
	}
	if strings.HasPrefix(input, "select") {
		statement.Type = StatementSelect
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

// --------- backend ---------
func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		if table.NumRows >= TableMaxRows {
			return ExecuteTableFull
		}
		cursor := table.TableEnd()
		rowSlot, _ := cursor.Value()
		statement.RowToInsert.Serialize(rowSlot)
		table.NumRows++
		return ExecuteSuccess
	case StatementSelect:
		cursor := table.TableStart()
		for i := uint32(0); i < table.NumRows; i++ {
			rowSlot, _ := cursor.Value()
			row := Deserialize(rowSlot)

			fmt.Printf("(%d, %s, %s)\n", row.ID, strings.Trim(string(row.Username[:]), "\x00"), strings.Trim(string(row.Email[:]), "\x00"))
			cursor.Advance()
		}
		return ExecuteSuccess
	}
	return ExecuteSuccess
}

func main() {
	fileName := "chibidb.db"
	table := DbOpen(fileName)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("chibidb > ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if input == "" {
			continue
		}

		if strings.HasPrefix(input, ".") {
			switch doMetaCommand(input, table) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				fmt.Printf("Unrecognized command '%s'\n", input)
				continue
			}
		}
		var statement Statement
		switch prepareStatement(input, &statement) {
		case PrepareSuccess:
		case PrepareSyntaxError:
			fmt.Println("Syntax Error. Could not parse statement.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
			continue
		}

		switch executeStatement(&statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
		case ExecuteTableFull:
			fmt.Println("Error: Table Full")
		}
	}
}

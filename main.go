package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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
	RowsPerSize   = PageSize / RowSize
	TableMaxRows  = RowsPerSize * TableMaxPages
)

type Row struct {
	ID       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}

type Table struct {
	NumRows uint32
	Pages   [TableMaxPages][]byte
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

func printPrompt() {
	fmt.Print("chibidb > ")
}

func doMetaCommand(input string) MetaCommandResult {
	if input == ".exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

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

func NewTable() *Table {
	return &Table{
		NumRows: 0,
	}
}

func (t *Table) rowSlot(rowNum uint32) ([]byte, error) {
	pageNum := rowNum / RowsPerSize

	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("page number out of bounds")
	}

	if t.Pages[pageNum] == nil {
		t.Pages[pageNum] = make([]byte, PageSize)
	}

	rowOffset := (rowNum % RowsPerSize) * RowSize

	return t.Pages[pageNum][rowOffset : rowOffset+RowSize], nil
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

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		if table.NumRows >= TableMaxRows {
			return ExecuteTableFull
		}
		rowSlot, _ := table.rowSlot(table.NumRows)
		statement.RowToInsert.Serialize(rowSlot)
		table.NumRows++
		return ExecuteSuccess
	case StatementSelect:
		for i := uint32(0); i < table.NumRows; i++ {
			rowSlot, _ := table.rowSlot(i)
			row := Deserialize(rowSlot)

			fmt.Printf("(%d, %s, %s)\n", row.ID, strings.Trim(string(row.Username[:]), "\x00"), strings.Trim(string(row.Email[:]), "\x00"))
		}
		return ExecuteSuccess
	}
	return ExecuteSuccess
}

func main() {
	table := NewTable()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		printPrompt()

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if input == "" {
			continue
		}

		if strings.HasPrefix(input, ".") {
			switch doMetaCommand(input) {
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

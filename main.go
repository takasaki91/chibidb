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
	RowSize      = IdSize + UsernameSize + EmailSize

	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages

	NodeTypeSize         = 1
	NodeTypeOffset       = 0
	IsRootSize           = 1
	IsRootOffset         = NodeTypeSize
	ParentPointerSize    = 4
	ParentPointerOffset  = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize

	LeafNodeNumCellsSize   = 4
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = CommonNodeHeaderSize + LeafNodeNumCellsSize

	LeafNodeKeySize       = 4
	LeafNodeKeyOffset     = 0
	LeafNodeValueSize     = RowSize
	LeafNodeValueOffset   = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize      = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      = LeafNodeSpaceForCells / LeafNodeCellSize
)

const (
	NodeTypeInternal = 0
	NodeTypeLeaf     = 1
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
	Pager *Pager
}

type Cursor struct {
	Table      *Table
	PageNum    int
	CellNum    uint32
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
	ExecuteDuplicateKey
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

func (p *Pager) Flush(pageNum int) {
	if p.Pages[pageNum] == nil {
		return
	}

	offset := int64(pageNum * PageSize)
	_, err := p.File.WriteAt(p.Pages[pageNum], offset)
	if err != nil {
		fmt.Println("Error writing to file.", err)
		os.Exit(1)
	}
}

// ----------- DB Connection ----------
func DbOpen(filename string) *Table {
	pager := NewPager(filename)

	if pager.FileLength == 0 {
		pager.GetPage(0)
		initializeLeadNode(pager.Pages[0])
	}

	return &Table{
		Pager: pager,
	}
}

func (t *Table) DbClose() {
	pager := t.Pager

	for i := 0; i < TableMaxPages; i++ {
		if pager.Pages[i] != nil {
			pager.Flush(i)
			pager.Pages[i] = nil
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

// ---------- Cursor Logic ----------

func (t *Table) TableStart() *Cursor {
	cursor := &Cursor{
		Table:   t,
		PageNum: 0,
		CellNum: 0,
	}
	rootNode := t.Pager.GetPage(0)
	numCells := leafNodeNumCells(rootNode)
	cursor.EndOfTable = (numCells == 0)

	return cursor
}

func (t *Table) TableEnd() *Cursor {
	rootNode := t.Pager.GetPage(0)
	numCells := leafNodeNumCells(rootNode)
	cursor := &Cursor{
		Table:      t,
		PageNum:    0,
		CellNum:    numCells,
		EndOfTable: true,
	}
	return cursor
}

func (t *Table) leafNodeFind(pageNum int, key uint32) *Cursor {
	node := t.Pager.GetPage(pageNum)
	numCells := leafNodeNumCells(node)

	cursor := &Cursor{
		Table:   t,
		PageNum: pageNum,
	}
	min := uint32(0)
	max := numCells

	for min != max {
		index := (min + max) / 2
		keyAtIndex := leafNodeKey(node, index)
		if key == keyAtIndex {
			cursor.CellNum = index
			return cursor
		}
		if key < keyAtIndex {
			max = index
		} else {
			min = index + 1
		}
	}
	cursor.CellNum = min
	return cursor
}
func (t *Table) tableFind(key uint32) *Cursor {
	rootPageNum := 0
	// rootNode := t.Pager.GetPage(rootPageNum)

	// if getNodeType(rootNode) == NodeTypeLeaf {.....}

	return t.leafNodeFind(rootPageNum, key)
}
func (c *Cursor) Value() ([]byte, error) {
	page := c.Table.Pager.GetPage(c.PageNum)

	return leafNodeValue(page, c.CellNum), nil
}

func (c *Cursor) Advance() {
	page := c.Table.Pager.GetPage(c.PageNum)
	numCells := leafNodeNumCells(page)

	c.CellNum += 1
	if c.CellNum >= numCells {
		c.EndOfTable = true
	}
}

// ---------- Node Helper Methods ----------

func leafNodeNumCells(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[LeafNodeNumCellsOffset:])
}

func leafNodeSetNumCells(node []byte, numCells uint32) {
	binary.LittleEndian.PutUint32(node[LeafNodeNumCellsOffset:], numCells)
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	offset := LeafNodeHeaderSize + cellNum*uint32(LeafNodeCellSize)
	return node[offset : offset+LeafNodeCellSize]
}

func leafNodeKey(node []byte, cellNum uint32) uint32 {
	cell := leafNodeCell(node, cellNum)
	return binary.LittleEndian.Uint32(cell)
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	cell := leafNodeCell(node, cellNum)
	return cell[LeafNodeKeyOffset+LeafNodeKeySize:]
}

func initializeLeadNode(node []byte) {
	leafNodeSetNumCells(node, 0)
	// TODO 簡易版らしい
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := cursor.Table.Pager.GetPage(cursor.PageNum)

	numCells := leafNodeNumCells(node)
	if numCells >= LeafNodeMaxCells {
		fmt.Println("Need to implement splitting a leaf node")
		os.Exit(1)
	}

	if cursor.CellNum < numCells {
		for i := numCells; i > cursor.CellNum; i-- {
			target := leafNodeCell(node, i)
			source := leafNodeCell(node, i-1)
			copy(target, source)
		}
	}

	leafNodeSetNumCells(node, numCells+1)

	cell := leafNodeCell(node, cursor.CellNum)
	binary.LittleEndian.PutUint32(cell[LeafNodeKeyOffset:], key)
	value.Serialize(leafNodeValue(node, cursor.CellNum))
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
		node := table.Pager.GetPage(0)
		numCells := leafNodeNumCells(node)
		if numCells >= LeafNodeMaxCells {
			return ExecuteTableFull
		}

		key := statement.RowToInsert.ID

		cursor := table.tableFind(key)

		if cursor.CellNum < numCells {
			keyAtIndex := leafNodeKey(node, cursor.CellNum)
			if keyAtIndex == key {
				return ExecuteDuplicateKey
			}
		}
		leafNodeInsert(cursor, statement.RowToInsert.ID, &statement.RowToInsert)
		return ExecuteSuccess
	case StatementSelect:
		cursor := table.TableStart()
		for !cursor.EndOfTable {
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
		case ExecuteDuplicateKey:
			fmt.Println("Error: Duplicate key.")
		}
	}
}

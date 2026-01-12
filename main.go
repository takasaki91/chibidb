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

	InternalNodeNumKeysSize      = 4
	InternalNodeNumKeysOffset    = CommonNodeHeaderSize
	InternalNodeRightChildSize   = 4
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       = CommonNodeHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize

	InternalNodeKeySize   = 4
	InternalNodeChildSize = 4
	InternalNodeCellSize  = InternalNodeChildSize + InternalNodeKeySize
	InternalNodeMaxSize   = 512
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
	NumPages   int
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

	numPages := int(fileLength / PageSize)
	if fileLength%PageSize != 0 {
		numPages++
	}
	return &Pager{
		File:       file,
		FileLength: fileLength,
		NumPages:   numPages,
	}
}

func (p *Pager) GetNewPageNum() int {
	pageIndex := p.NumPages
	p.NumPages++
	return pageIndex
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
		initializeLeafNode(pager.Pages[0])
		setNodeRoot(pager.Pages[0], true)
		pager.NumPages = 1
	}

	return &Table{
		Pager: pager,
	}
}

func (t *Table) DbClose() {
	pager := t.Pager

	for i := 0; i < pager.NumPages; i++ {
		if pager.Pages[i] != nil {
			pager.Flush(i)
			pager.Pages[i] = nil
		}
	}

	pager.File.Close()
}

// ---------- Core Logic & Tree Management ----------

func (t *Table) createNewRoot(rightChildPageNum int) {
	pager := t.Pager
	root := pager.GetPage(0)

	rightChild := pager.GetPage(rightChildPageNum)

	leftChildPageNum := pager.GetNewPageNum()
	leftChild := pager.GetPage(leftChildPageNum)
	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	initializeInternalNode(root)
	setNodeRoot(root, true)
	internalNodeSetNumKeys(root, 1)
	internalNodeSetChild(root, 0, uint32(leftChildPageNum))

	rightChildKey := leafNodeKey(rightChild, 0)
	internalNodeSetKey(root, 0, rightChildKey)
	internalNodeSetRightChild(root, uint32(rightChildPageNum))

	setNodeParent(leftChild, 0)
	setNodeParent(rightChild, 0)
}

func (t *Table) leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := t.Pager.GetPage(cursor.PageNum)
	oldMax := leafNodeNumCells(oldNode)

	newPageNum := t.Pager.GetNewPageNum()
	newNode := t.Pager.GetPage(newPageNum)
	initializeLeafNode(newNode)

	setNodeParent(newNode, nodeParent(oldNode))

	splitIndex := LeafNodeMaxCells / 2

	for i := int32(LeafNodeMaxCells); i >= 0; i-- {
		var destinationNode []byte
		var indexWithinNode uint32

		if i >= int32(splitIndex) {
			destinationNode = newNode
			indexWithinNode = uint32(i) - uint32(splitIndex)
		} else {
			destinationNode = oldNode
			indexWithinNode = uint32(i)
		}

		if uint32(i) == cursor.CellNum {
			leafNodeSetNumCells(destinationNode, leafNodeNumCells(destinationNode)+1)
			cell := leafNodeCell(destinationNode, indexWithinNode)
			binary.LittleEndian.PutUint32(cell[LeafNodeKeyOffset:], key)
			value.Serialize(leafNodeValue(destinationNode, indexWithinNode))
		} else if uint32(i) > cursor.CellNum {
			copyCell(oldNode, uint32(i-1), destinationNode, indexWithinNode)
		} else {
			copyCell(oldNode, uint32(i), destinationNode, indexWithinNode)
		}
	}
	leafNodeSetNumCells(oldNode, uint32(splitIndex))

	if isNodeRoot(oldNode) {
		t.createNewRoot(newPageNum)
	} else {
		parentPageNum := int(nodeParent(oldNode))

		newMax := leafNodeKey(newNode, 0)
		parent := t.Pager.GetPage(parentPageNum)

		t.internalNodeInsert(parent, parentPageNum, uint32(newPageNum), oldMax, newMax)
	}
}

func (t *Table) internalNodeInsert(node []byte, parentPageNum int, childPageNum uint32, oldMaxKey uint32, newKey uint32) {
	numKeys := internalNodeNumKeys(node)
	if numKeys >= InternalNodeMaxSize {
		fmt.Println("Need to implement splitting internal node")
		os.Exit(1)
	}
	rightChildPageNum := internalNodeRightChild(node)

	cursor := t.internalNodeFind(parentPageNum, oldMaxKey)
	index := cursor.CellNum

	internalNodeSetNumKeys(node, numKeys+1)
	if numKeys > 0 && index < numKeys {
		// TODO
	}
	internalNodeSetChild(node, numKeys, rightChildPageNum)
	internalNodeSetKey(node, numKeys, newKey)
	internalNodeSetRightChild(node, childPageNum)
}

func copyCell(src []byte, srcIndex uint32, dst []byte, dstIndex uint32) {
	leafNodeSetNumCells(dst, leafNodeNumCells(dst)+1)
	dstCell := leafNodeCell(dst, dstIndex)
	srcCell := leafNodeCell(src, srcIndex)
	copy(dstCell, srcCell)
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

func (t *Table) internalNodeFind(pageNum int, key uint32) *Cursor {
	node := t.Pager.GetPage(pageNum)
	numKeys := internalNodeNumKeys(node)

	min := uint32(0)
	max := numKeys

	for min != max {
		index := (min + max) / 2
		keyAtIndex := internalNodeKey(node, index)
		if key <= keyAtIndex {
			max = index
		} else {
			min = index + 1
		}
	}

	cursor := &Cursor{
		Table:   t,
		PageNum: pageNum,
		CellNum: min,
	}
	return cursor
}

func (t *Table) TableStart() *Cursor {
	cursor := t.tableFind(0)
	return cursor
}

func (t *Table) TableEnd() *Cursor {
	return t.TableStart()
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
	rootNode := t.Pager.GetPage(rootPageNum)

	if getNodeType(rootNode) == NodeTypeLeaf {
		return t.leafNodeFind(rootPageNum, key)
	} else {
		return t.findKeyInInternal(rootPageNum, key)
	}
}

func (t *Table) findKeyInInternal(pageNum int, key uint32) *Cursor {
	node := t.Pager.GetPage(pageNum)
	numKeys := internalNodeNumKeys(node)

	min := uint32(0)
	max := numKeys

	for min != max {
		index := (min + max) / 2
		keyAtIndex := internalNodeKey(node, index)
		if key <= keyAtIndex {
			max = index
		} else {
			min = index + 1
		}
	}

	childNum := min
	childPageNum := internalNodeChild(node, childNum)

	childNode := t.Pager.GetPage(int(childPageNum))
	if getNodeType(childNode) == NodeTypeLeaf {
		return t.leafNodeFind(int(childPageNum), key)
	} else {
		return t.findKeyInInternal(int(childPageNum), key)
	}
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

// ---------- Common Helper ----------

func getNodeType(node []byte) uint32 {
	return uint32(node[NodeTypeOffset])
}

func setNodeType(node []byte, qtype uint32) {
	node[NodeTypeOffset] = byte(qtype)
}

func isNodeRoot(node []byte) bool {
	return node[IsRootOffset] == 1
}

func setNodeRoot(node []byte, isRoot bool) {
	var val byte
	if isRoot {
		val = 1
	}
	node[IsRootOffset] = val
}

func nodeParent(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[ParentPointerOffset:])
}

func setNodeParent(node []byte, parent uint32) {
	binary.LittleEndian.PutUint32(node[ParentPointerOffset:], parent)
}

// ---------- Leaf Node Helper Methods ----------

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

func initializeLeafNode(node []byte) {
	setNodeType(node, NodeTypeLeaf)
	setNodeRoot(node, false)
	leafNodeSetNumCells(node, 0)
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := cursor.Table.Pager.GetPage(cursor.PageNum)
	numCells := leafNodeNumCells(node)

	if numCells >= LeafNodeMaxCells {
		cursor.Table.leafNodeSplitAndInsert(cursor, key, value)
		return
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

// ---------- Internal Node Helper ----------

func internalNodeNumKeys(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[InternalNodeNumKeysOffset:])
}

func internalNodeSetNumKeys(node []byte, numKeys uint32) {
	binary.LittleEndian.PutUint32(node[InternalNodeNumKeysOffset:], numKeys)
}

func internalNodeRightChild(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[InternalNodeRightChildOffset:])
}

func internalNodeSetRightChild(node []byte, rightChild uint32) {
	binary.LittleEndian.PutUint32(node[InternalNodeRightChildOffset:], rightChild)
}

func internalNodeCell(node []byte, cellNum uint32) []byte {
	offset := InternalNodeHeaderSize + cellNum*uint32(InternalNodeCellSize)
	return node[offset : offset+InternalNodeCellSize]
}

func internalNodeChild(node []byte, cellNum uint32) uint32 {
	numKeys := internalNodeNumKeys(node)
	if cellNum > numKeys {
		fmt.Printf("Tried to access child_num %d > num_keys %d", cellNum, numKeys)
		os.Exit(1)
	} else if cellNum == numKeys {
		return internalNodeRightChild(node)
	} else {
		cell := internalNodeCell(node, cellNum)
		return binary.LittleEndian.Uint32(cell)
	}
	return 0
}

func internalNodeKey(node []byte, cellNum uint32) uint32 {
	cell := internalNodeCell(node, cellNum)
	return binary.LittleEndian.Uint32(cell[InternalNodeChildSize:])
}

func internalNodeSetKey(node []byte, cellNum uint32, key uint32) {
	cell := internalNodeCell(node, cellNum)
	binary.LittleEndian.PutUint32(cell[InternalNodeChildSize:], key)
}

func internalNodeSetChild(node []byte, cellNum uint32, childPageNum uint32) {
	cell := internalNodeCell(node, cellNum)
	binary.LittleEndian.PutUint32(cell, childPageNum)
}

func initializeInternalNode(node []byte) {
	setNodeType(node, NodeTypeInternal)
	setNodeRoot(node, false)
	internalNodeSetNumKeys(node, 0)
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", RowSize)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", CommonNodeHeaderSize)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNodeHeaderSize)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNodeCellSize)
	fmt.Printf("LEAF_NODE_SPACE_FOR_SCALE: %d\n", LeafNodeSpaceForCells)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

func printLeafNode(node []byte) {
	numCells := leafNodeNumCells(node)
	fmt.Printf("leaf (size %d)\n", numCells)

	for i := uint32(0); i < numCells; i++ {
		key := leafNodeKey(node, i)
		fmt.Printf(" - %d\n", key)
	}
}
func indent(level int) {
	for i := 0; i < level; i++ {
		fmt.Print("  ")
	}
}
func printTree(pager *Pager, pageNum int, level int) {
	node := pager.GetPage(pageNum)
	nodeType := getNodeType(node)

	indent(level)

	switch nodeType {
	case NodeTypeLeaf:
		numCells := leafNodeNumCells(node)
		fmt.Printf("- leaf (size %d) [Page %d]\n", numCells, pageNum)
		for i := uint32(0); i < numCells; i++ {
			indent(level + 1)
			fmt.Printf("- %d\n", leafNodeKey(node, i))
		}
	case NodeTypeInternal:
		numKeys := internalNodeNumKeys(node)
		fmt.Printf("- internal (keys %d) [Page %d]\n", numKeys, pageNum)
		for i := uint32(0); i < numKeys; i++ {
			child := internalNodeChild(node, i)
			printTree(pager, int(child), level+1)

			indent(level + 1)
			fmt.Printf("- key %d\n", internalNodeKey(node, i))
		}
		child := internalNodeRightChild(node)
		printTree(pager, int(child), level+1)
	}
}

// --------- front ---------

func doMetaCommand(input string, table *Table) MetaCommandResult {
	if input == ".exit" {
		table.DbClose()
		os.Exit(0)
	}
	if input == ".btree" {
		fmt.Println("Tree:")
		printTree(table.Pager, 0, 0)
		return MetaCommandSuccess
	}
	if input == ".constants" {
		fmt.Println("Constants:")
		printConstants()
		return MetaCommandSuccess
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
		key := statement.RowToInsert.ID
		cursor := table.tableFind(key)

		node := table.Pager.GetPage(cursor.PageNum)
		numCells := leafNodeNumCells(node)

		if cursor.CellNum < numCells {
			keyAtIndex := leafNodeKey(node, cursor.CellNum)
			if keyAtIndex == key {
				return ExecuteDuplicateKey
			}
		}
		leafNodeInsert(cursor, statement.RowToInsert.ID, &statement.RowToInsert)
		return ExecuteSuccess
	case StatementSelect:
		cursor := table.tableFind(0)
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

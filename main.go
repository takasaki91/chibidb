package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognizedCommand
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type StatementType
}

func printPrompt() {
	fmt.Print("chibidb > ")
}

func doMetaCommand(input string) MetaCommandResult {
	if input == ".exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	inputlower := strings.ToLower(input)

	if strings.HasPrefix(inputlower, "insert") {
		statement.Type = StatementInsert
		return PrepareSuccess
	}
	if strings.HasPrefix(inputlower, "select") {
		statement.Type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeStatement(statement *Statement) {
	switch statement.Type {
	case StatementInsert:
		fmt.Println("This is where we would do an insert")
	case StatementSelect:
		fmt.Println("This is where we would do a select")
	}
}
func main() {
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

		executeStatement(&statement)
		fmt.Println("Executed.")
	}
}

package util

import (
	"fmt"
	"strings"
)

func EncloseValue(value any, escapeFunc func(string) string) string {
	var buf strings.Builder
	if value == nil {
		buf.WriteString("NULL")
	} else {
		buf.Grow(len(value.(string)) + 2)
		buf.WriteString("'")
		buf.WriteString(escapeFunc(value.(string)))
		buf.WriteString("'")
	}

	return buf.String()
}

func EncloseValues(values []any, escapeFunc func(string) string) (list []string) {
	for i, _ := range values {
		list = append(list, EncloseValue(values[i], escapeFunc))
	}
	return
}

func GenerateClause(fields []string, values []string, quote, sep string) (string, error) {
	//生成where的条件时，使用and分隔
	//生成update set字句时，使用,分隔
	if len(fields) != len(values) {
		return "", fmt.Errorf("fields and values length not equal")
	}

	var buf strings.Builder
	totalLength := estimateTotalLength(fields, values, len(sep))
	buf.Grow(totalLength)
	for i, f := range fields {
		if i > 0 {
			buf.WriteString(fmt.Sprintf(" %s %s=%s", sep, EncloseStr(f, quote), values[i]))
		} else {
			buf.WriteString(fmt.Sprintf("%s=%s", EncloseStr(f, quote), values[i]))

		}
	}

	return buf.String(), nil
}

func estimateTotalLength(fields []string, values []string, sepLen int) int {
	total := 0
	for i := range fields {
		total += len(fields[i]) + len((values[i])) + 3 // 列名2个引号 + 1个等号
		if i > 0 {
			total += sepLen + 2 // 分隔符前后添加2个空格（为了兼容and分隔，前后都要加空格）
		}
	}
	return total
}

package scheduler

import (
	"fmt"
	"strconv"
	"strings"
)

func GenerateWorker(source Source) []byte {
	var builder strings.Builder
	builder.WriteString("// Code generated from the authoritative .gooo source; DO NOT EDIT.\n")
	builder.WriteString("package generatedworker\n\n")
	fmt.Fprintf(&builder, "const SourceSchema = %s\n", strconv.Quote(source.Schema))
	fmt.Fprintf(&builder, "const SourceDigest = %s\n", strconv.Quote(source.SourceDigest))
	fmt.Fprintf(&builder, "const ConcurrencyBound = %d\n\n", source.ConcurrencyBound)
	builder.WriteString("type Lock struct {\n\tID string\n\tCoordinate string\n\tDigest string\n\tDependencies []string\n\tBehavior string\n}\n\n")
	builder.WriteString("var Locks = []Lock{\n")
	for _, lock := range source.Locks {
		fmt.Fprintf(&builder, "\t{ID: %s, Coordinate: %s, Digest: %s, Dependencies: []string{", strconv.Quote(lock.ID), strconv.Quote(lock.Coordinate), strconv.Quote(lock.Digest))
		for index, dependency := range lock.Dependencies {
			if index > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(strconv.Quote(dependency))
		}
		fmt.Fprintf(&builder, "}, Behavior: %s},\n", strconv.Quote(lock.Behavior))
	}
	builder.WriteString("}\n\n")
	builder.WriteString("var CanonicalOrder = []string{\n")
	for _, id := range source.CanonicalOrder {
		fmt.Fprintf(&builder, "\t%s,\n", strconv.Quote(id))
	}
	builder.WriteString("}\n")
	return []byte(builder.String())
}

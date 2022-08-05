package nats

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

const (
	readURN       = "beam:transform:io.reflek.gulfstream:nats:v1"
	expansionAddr = "localhost:3333"
)

type natsConnectionConfiguration struct {
	ServerUri string
	Subject   string
}

func Read(s beam.Scope, serverUri, subject string) beam.PCollection {
	config := natsConnectionConfiguration{
		ServerUri: serverUri,
		Subject:   subject,
	}

	pl := beam.CrossLanguagePayload(config)
	t := reflectx.ByteSlice
	outT := beam.UnnamedOutput(typex.New(t))
	out := beam.CrossLanguage(s, readURN, pl, expansionAddr, nil, outT)
	return out[beam.UnnamedOutputTag()]
}

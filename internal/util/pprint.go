package util

import (
	"encoding/json"
	"fmt"

	"github.com/TylerBrock/colorjson"
	"github.com/breezewish/gscache/internal/log"
	"github.com/fatih/color"
	"go.uber.org/zap"
)

func ObjectToMapViaJSONSerde(data any) (map[string]any, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(bytes, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func PrettyPrintJSON(data any) {
	m, err := ObjectToMapViaJSONSerde(data)
	if err != nil {
		log.Error("Failed to convert data to JSON",
			zap.Error(err))
		return
	}
	formatter := colorjson.NewFormatter()
	formatter.Indent = 2
	formatter.DisabledColor = color.NoColor
	s, _ := formatter.Marshal(m)
	fmt.Println(string(s))
}

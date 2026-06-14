package consul

import "gopkg.in/yaml.v3"

// DeepMergeMaps merges override into base; override values win.
func DeepMergeMaps(base, override map[string]any) map[string]any {
	result := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		result[k] = v
	}
	for k, ov := range override {
		bv, ok := result[k]
		if !ok {
			result[k] = ov
			continue
		}
		bm, bOK := bv.(map[string]any)
		om, oOK := ov.(map[string]any)
		if bOK && oOK {
			result[k] = DeepMergeMaps(bm, om)
			continue
		}
		result[k] = ov
	}
	return result
}

// DeepMergeYAML parses two YAML documents, deep-merges them, and re-marshals.
func DeepMergeYAML(base, override []byte) ([]byte, error) {
	var baseMap, overrideMap map[string]any
	if err := yaml.Unmarshal(base, &baseMap); err != nil {
		return override, nil
	}
	if err := yaml.Unmarshal(override, &overrideMap); err != nil {
		return base, nil
	}
	merged := DeepMergeMaps(baseMap, overrideMap)
	return yaml.Marshal(merged)
}

package helper

import (
	gaemonhelper "eaciit/melon/gaemon/helper"
)

func GetStringsFromTkM(data map[string]interface{}, key string) []string {
	ls := []string{}
	if _, ok := data[key]; ok {
		if v, ok2 := data[key].([]string); ok2 {
			return v
		} else if _, ok2 := data[key].([]interface{}); ok2 {
			for _, iv := range data[key].([]interface{}) {
				if v, ok3 := iv.(string); ok3 {
					ls = append(ls, v)
				}
			}
		}
	}
	return ls
}
func GetStringFromTkM(data map[string]interface{}, key string) string {
	if _, ok := data[key]; ok {
		return gaemonhelper.GetFirstStringFromSlice(data[key])
	}
	return ""
}

// tkm long/int to long
func GetLongFromTkM(data map[string]interface{}, key string) int64 {
	if _, ok := data[key]; ok {
		if v, ok2 := data[key].(int32); ok2 {
			return int64(v)
		} else if v, ok2 := data[key].(int64); ok2 {
			return v
		} else if v, ok2 := data[key].(float32); ok2 {
			return int64(v)
		} else if v, ok2 := data[key].(float64); ok2 {
			return int64(v)
		} else if vs, ok2 := data[key].([]int32); ok2 {
			for _, v := range vs {
				return int64(v)
			}
			return 0
		} else if vs, ok2 := data[key].([]int64); ok2 {
			for _, v := range vs {
				return v
			}
			return 0
		} else if vs, ok2 := data[key].([]float32); ok2 {
			for _, v := range vs {
				return int64(v)
			}
			return 0
		} else if vs, ok2 := data[key].([]float64); ok2 {
			for _, v := range vs {
				return int64(v)
			}
			return 0
		}
	}
	return 0
}

// tkm []int/[]long to tkm []long
func GetLongsFromTkM(data map[string]interface{}, key string) []int64 {
	ls := []int64{}
	if _, ok := data[key]; ok {
		if _, ok2 := data[key].([]int32); ok2 {
			for _, v := range data[key].([]int32) {
				ls = append(ls, int64(v))
			}
		} else if _, ok2 := data[key].([]float32); ok2 {
			for _, v := range data[key].([]float32) {
				ls = append(ls, int64(v))
			}
		} else if _, ok2 := data[key].([]float64); ok2 {
			for _, v := range data[key].([]float64) {
				ls = append(ls, int64(v))
			}
		} else if v, ok2 := data[key].([]int64); ok2 {
			return v
		} else if _, ok2 := data[key].([]interface{}); ok2 {
			for _, iv := range data[key].([]interface{}) {
				if v, ok3 := iv.(int32); ok3 {
					ls = append(ls, int64(v))
				} else if v, ok3 := iv.(int64); ok3 {
					ls = append(ls, v)
				} else if v, ok3 := iv.(float64); ok3 {
					ls = append(ls, int64(v))
				} else if v, ok3 := iv.(float32); ok3 {
					ls = append(ls, int64(v))
				}
			}
		}
	}
	return ls
}

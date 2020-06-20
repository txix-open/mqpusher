package source

import (
	"database/sql/driver"
	"github.com/jackc/pgtype"
	errors "golang.org/x/xerrors"
)

var (
	errUndefined = errors.New("cannot encode status undefined")
	errBadStatus = errors.New("invalid status")
)

func RegisterTypes(ci *pgtype.ConnInfo) {
	ci.RegisterDataType(pgtype.DataType{Value: &JSON{}, Name: "json", OID: pgtype.JSONOID})
	ci.RegisterDataType(pgtype.DataType{Value: &JSONB{}, Name: "jsonb", OID: pgtype.JSONBOID})
}

// Borrowed from github.com/jackc/pgtype to replace std json with jsoniter
type JSON struct {
	Bytes  []byte
	Status pgtype.Status
}

func (dst *JSON) Set(src interface{}) error {
	if src == nil {
		*dst = JSON{Status: pgtype.Null}
		return nil
	}

	switch value := src.(type) {
	case string:
		*dst = JSON{Bytes: []byte(value), Status: pgtype.Present}
	case *string:
		if value == nil {
			*dst = JSON{Status: pgtype.Null}
		} else {
			*dst = JSON{Bytes: []byte(*value), Status: pgtype.Present}
		}
	case []byte:
		if value == nil {
			*dst = JSON{Status: pgtype.Null}
		} else {
			*dst = JSON{Bytes: value, Status: pgtype.Present}
		}
	// Encode* methods are defined on *JSON. If JSON is passed directly then the
	// struct itself would be encoded instead of Bytes. This is clearly a footgun
	// so detect and return an error. See https://github.com/jackc/pgx/issues/350.
	case JSON:
		return errors.New("use pointer to pgtype.JSON instead of value")
	// Same as above but for JSONB (because they share implementation)
	case JSONB:
		return errors.New("use pointer to pgtype.JSONB instead of value")

	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}
		*dst = JSON{Bytes: buf, Status: pgtype.Present}
	}

	return nil
}

func (dst *JSON) Get() interface{} {
	switch dst.Status {
	case pgtype.Present:
		var i interface{}
		err := json.Unmarshal(dst.Bytes, &i)
		if err != nil {
			return dst
		}
		return i
	case pgtype.Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *JSON) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		if src.Status == pgtype.Present {
			*v = string(src.Bytes)
		} else {
			return errors.Errorf("cannot assign non-present status to %T", dst)
		}
	case **string:
		if src.Status == pgtype.Present {
			s := string(src.Bytes)
			*v = &s
			return nil
		} else {
			*v = nil
			return nil
		}
	case *[]byte:
		if src.Status != pgtype.Present {
			*v = nil
		} else {
			buf := make([]byte, len(src.Bytes))
			copy(buf, src.Bytes)
			*v = buf
		}
	default:
		data := src.Bytes
		if data == nil || src.Status != pgtype.Present {
			data = []byte("null")
		}

		return json.Unmarshal(data, dst)
	}

	return nil
}

func (dst *JSON) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = JSON{Status: pgtype.Null}
		return nil
	}

	*dst = JSON{Bytes: src, Status: pgtype.Present}
	return nil
}

func (dst *JSON) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (src JSON) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return append(buf, src.Bytes...), nil
}

func (src JSON) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return src.EncodeText(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *JSON) Scan(src interface{}) error {
	if src == nil {
		*dst = JSON{Status: pgtype.Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src JSON) Value() (driver.Value, error) {
	switch src.Status {
	case pgtype.Present:
		return src.Bytes, nil
	case pgtype.Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}

func (src JSON) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case pgtype.Present:
		return src.Bytes, nil
	case pgtype.Null:
		return []byte("null"), nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return nil, errBadStatus
}

func (dst *JSON) UnmarshalJSON(b []byte) error {
	if b == nil || string(b) == "null" {
		*dst = JSON{Status: pgtype.Null}
	} else {
		*dst = JSON{Bytes: b, Status: pgtype.Present}
	}
	return nil

}

type JSONB JSON

func (dst *JSONB) Set(src interface{}) error {
	return (*JSON)(dst).Set(src)
}

func (dst *JSONB) Get() interface{} {
	return (*JSON)(dst).Get()
}

func (src *JSONB) AssignTo(dst interface{}) error {
	return (*JSON)(src).AssignTo(dst)
}

func (dst *JSONB) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	return (*JSON)(dst).DecodeText(ci, src)
}

func (dst *JSONB) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = JSONB{Status: pgtype.Null}
		return nil
	}

	if len(src) == 0 {
		return errors.Errorf("jsonb too short")
	}

	if src[0] != 1 {
		return errors.Errorf("unknown jsonb version number %d", src[0])
	}

	*dst = JSONB{Bytes: src[1:], Status: pgtype.Present}
	return nil

}

func (src JSONB) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return (JSON)(src).EncodeText(ci, buf)
}

func (src JSONB) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	buf = append(buf, 1)
	return append(buf, src.Bytes...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *JSONB) Scan(src interface{}) error {
	return (*JSON)(dst).Scan(src)
}

// Value implements the database/sql/driver Valuer interface.
func (src JSONB) Value() (driver.Value, error) {
	return (JSON)(src).Value()
}

func (src JSONB) MarshalJSON() ([]byte, error) {
	return (JSON)(src).MarshalJSON()
}

func (dst *JSONB) UnmarshalJSON(b []byte) error {
	return (*JSON)(dst).UnmarshalJSON(b)
}
